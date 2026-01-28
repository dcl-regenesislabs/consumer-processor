import { IConfigComponent, Lifecycle } from '@well-known-components/interfaces'
import { setupRouter } from './controllers/routes'
import { godotOptimizer, ProcessReport } from './runners/godot-optimizer'
import { godotGenerateSceneImages } from './runners/minimap-generator/godot-generate-scene-images'
import { AppComponents, GlobalContext, TestComponents } from './types'
import { generateCrdt } from './runners/crdt-runner'
import { generateImposter } from './runners/imposter-runner'
import { DeploymentToSqs } from '@dcl/schemas/dist/misc/deployments-to-sqs'
import type { IFetchComponent } from '@well-known-components/http-server'

export const validProcessMethods = [
  'log',
  'godot_minimap',
  'godot_optimizer',
  'generate_crdt',
  'generate_imposters'
] as const
export type ProcessMethod = (typeof validProcessMethods)[number]

export async function getProcessMethod(config: IConfigComponent): Promise<ProcessMethod> {
  const processMethod = (await config.getString('PROCESS_METHOD')) || 'log'
  if (!validProcessMethods.includes(processMethod as any)) {
    throw new Error(`Unknown process method: ${processMethod}`)
  }
  return processMethod as ProcessMethod
}

type EntityType = 'scene' | 'wearable' | 'emote'

async function fetchThumbnailUrl(
  fetch: IFetchComponent,
  job: DeploymentToSqs,
  entityType: EntityType
): Promise<string | undefined> {
  try {
    const entityId = job.entity.entityId
    const contentServerUrl =
      job.contentServerUrls && job.contentServerUrls.length > 0
        ? job.contentServerUrls[0]
        : 'https://peer.decentraland.org/content'

    // Fetch entity metadata
    const response = await fetch.fetch(`${contentServerUrl}/entities/active`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids: [entityId] })
    })

    if (!response.ok) {
      await response.text().catch(() => {})
      return undefined
    }

    const entities = (await response.json()) as Array<{
      id: string
      content: Array<{ file: string; hash: string }>
      metadata?: {
        display?: { navmapThumbnail?: string }
        thumbnail?: string
      }
    }>

    if (!entities || entities.length === 0) {
      return undefined
    }

    const entity = entities[0]
    let thumbnailFile: string | undefined

    // Get thumbnail file path based on entity type
    if (entityType === 'scene') {
      thumbnailFile = entity.metadata?.display?.navmapThumbnail
    } else {
      thumbnailFile = entity.metadata?.thumbnail
    }

    if (!thumbnailFile) {
      return undefined
    }

    // Find the hash for the thumbnail file
    const thumbnailContent = entity.content.find((c) => c.file.toLowerCase() === thumbnailFile?.toLowerCase())

    if (!thumbnailContent) {
      return undefined
    }

    // Return the full URL to the thumbnail
    return `${contentServerUrl}/contents/${thumbnailContent.hash}`
  } catch {
    // Non-blocking - return undefined on any error
    return undefined
  }
}

// this function wires the business logic (adapters & controllers) with the components (ports)
export async function main(program: Lifecycle.EntryPointParameters<AppComponents | TestComponents>) {
  const { components, startComponents } = program
  const globalContext: GlobalContext = {
    components
  }

  // wire the HTTP router (make it automatic? TBD)
  const router = await setupRouter(globalContext)
  // register routes middleware
  components.server.use(router.middleware())
  // register not implemented/method not allowed/cors responses middleware
  components.server.use(router.allowedMethods())
  // set the context to be passed to the handlers
  components.server.setContext(globalContext)

  // start ports: db, listeners, synchronizations, etc
  await startComponents()

  const logger = components.logs.getLogger('main-loop')
  const processMethod = await getProcessMethod(components.config)
  logger.info('Process method: ', { processMethod })

  components.runner.runTask(async (opt) => {
    while (opt.isRunning) {
      await components.taskQueue.consumeAndProcessJob(async (job, message) => {
        const sceneId = job.entity.entityId
        const entityType = (job.entity as any).entityType || 'scene'
        const startedAt = new Date().toISOString()
        const startTime = Date.now()
        const isPriority = message.isPriority

        if (isPriority) {
          logger.info('Processing PRIORITY job', { sceneId })
        }

        // Report job start
        components.monitoringReporter.reportHeartbeat({
          status: 'processing',
          currentSceneId: sceneId,
          currentStep: 'Starting',
          progressPercent: 0,
          startedAt,
          isPriority,
          entityType
        })

        let processReport: ProcessReport | null = null

        try {
          switch (processMethod) {
            case 'godot_optimizer':
              processReport = await godotOptimizer(job, message, globalContext.components)
              break
            case 'godot_minimap':
              await godotGenerateSceneImages(globalContext.components, job, message)
              break
            case 'generate_crdt':
              await generateCrdt(globalContext.components, job, message)
              break
            case 'generate_imposters':
              await generateImposter(globalContext.components, job, message)
              break
            case 'log':
              logger.info('Consume and Process: ', { job: JSON.stringify(job), message: JSON.stringify(message) })
              break
          }

          // Fetch thumbnail URL for the report (non-blocking)
          const thumbnailUrl = await fetchThumbnailUrl(components.fetch, job, entityType as EntityType)

          // Report job success
          components.monitoringReporter.reportJobComplete({
            sceneId,
            status: 'success',
            startedAt,
            completedAt: new Date().toISOString(),
            durationMs: Date.now() - startTime,
            isPriority,
            entityType,
            thumbnailUrl,
            reportJson: processReport || undefined
          })
        } catch (error) {
          logger.error(`Error processing job ${job.entity.entityId}`)
          logger.error(error as any)

          // Extract report from error if available (godot-optimizer attaches it)
          const errorWithReport = error as Error & { report?: ProcessReport }
          if (errorWithReport.report) {
            processReport = errorWithReport.report
          }

          // Fetch thumbnail URL for the report (non-blocking)
          const thumbnailUrl = await fetchThumbnailUrl(components.fetch, job, entityType as EntityType)

          // Report job failure
          components.monitoringReporter.reportJobComplete({
            sceneId,
            status: 'failed',
            startedAt,
            completedAt: new Date().toISOString(),
            durationMs: Date.now() - startTime,
            errorMessage: error instanceof Error ? error.message : 'Unknown error',
            isPriority,
            entityType,
            thumbnailUrl,
            reportJson: processReport || undefined
          })
        }
      })
    }
  })
}
