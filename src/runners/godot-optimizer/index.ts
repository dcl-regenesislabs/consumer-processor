import { DeploymentToSqs } from '@dcl/schemas/dist/misc/deployments-to-sqs'
import fs from 'fs/promises'
import path from 'path'
import { TaskQueueMessage } from '../../adapters/sqs'
import { AppComponents } from '../../types'
import { AssetType } from '../../adapters/asset-server'
import { clearGodotCache, preloadEntityContent } from './content-preloader'

// Extended type that includes entityType and profile data from the producer
type DeploymentWithType = DeploymentToSqs & {
  entity: {
    entityType?: string
  }
  _profileData?: {
    originalEntityId: string
    gltfFile: string
    contentMapping: Record<string, string>
    contentBaseUrl: string
  }
}

type EntityType = 'scene' | 'wearable' | 'emote'

export type ProcessReport = {
  entityId: string
  entityType: EntityType
  contentServerUrl: string
  startedAt: Date
  finishedAt: Date | null
  errors: string[]
  godotLogs: string[]
  godotProcessLogs: string[]
  individualAssets: {
    total: number
    successful: number
    failed: number
  }
  result: {
    success: boolean
    batchId?: string
    optimizedAssets?: number
    metadataZipPath?: string
    individualZips?: string[]
  } | null
}

export async function godotOptimizer(
  entity: DeploymentToSqs,
  _msg: TaskQueueMessage,
  components: Pick<AppComponents, 'logs' | 'config' | 'storage' | 'assetServer' | 'fetch'>
): Promise<ProcessReport | null> {
  const { logs, assetServer, storage } = components
  const logger = logs.getLogger('godot-optimizer')

  // Cast to extended type to access entityType
  const entityWithType = entity as DeploymentWithType
  const entityType = (entityWithType.entity.entityType as EntityType) || 'scene'

  logger.info('Processing entity', {
    entityId: entity.entity.entityId,
    entityType
  })

  const tempDir = path.join(process.cwd(), 'temp')
  let report: ProcessReport | null = null

  // Ensure Godot is running (first call after container start, or after a crash)
  if (!(await assetServer.isReady())) {
    logger.info('Godot asset-server not running, starting it...')
    const started = await assetServer.restartGodot()
    if (!started) {
      throw new Error('Failed to start Godot asset-server')
    }
  }

  try {
    switch (entityType) {
      case 'scene':
        report = await processScene(entity, components)
        break
      case 'wearable':
      case 'emote':
        report = await processWearableOrEmote(entity, entityType, components)
        break
      default:
        logger.warn('Unknown entity type, defaulting to scene processing', { entityType })
        report = await processScene(entity, components)
    }
  } finally {
    // Get Godot process stdout/stderr before restart (they will be cleared on restart)
    const godotProcessLogs = assetServer.getGodotLogs()
    if (godotProcessLogs.length > 0) {
      logger.info('Captured Godot process logs', { lineCount: godotProcessLogs.length })
    }

    // Add Godot process logs to separate field in report
    if (report) {
      report.godotProcessLogs = godotProcessLogs
      await storeReport(report, tempDir, storage, logger)
    }

    // Clear Godot content cache to free disk space
    await clearGodotCache(logger).catch((err) => {
      logger.warn('Failed to clear Godot cache', {
        error: err instanceof Error ? err.message : String(err)
      })
    })

    // Restart Godot after each entity to free memory
    logger.info('Processing complete, restarting Godot to free memory', {
      entityId: entity.entity.entityId
    })
    await assetServer.restartGodot()
  }

  // Throw error if processing failed so service.ts reports correct status
  // But first, save the report reference so it can be returned
  const finalReport = report
  if (finalReport && finalReport.result && !finalReport.result.success) {
    const errorMsg = finalReport.errors.length > 0 ? finalReport.errors.join('; ') : 'Processing failed'
    // Store report in a way that service.ts can access it even on error
    // We'll throw but also return the report via a custom error
    const error = new Error(errorMsg) as Error & { report?: ProcessReport }
    error.report = finalReport
    throw error
  }

  return finalReport
}

/**
 * Process a scene entity - creates individual ZIPs for each asset
 */
async function processScene(
  entity: DeploymentToSqs,
  components: Pick<AppComponents, 'logs' | 'config' | 'storage' | 'assetServer' | 'fetch'>
): Promise<ProcessReport> {
  const { logs, storage, assetServer, config } = components
  const logger = logs.getLogger('godot-optimizer:scene')

  const entityId = entity.entity.entityId
  const contentServerUrl =
    entity.contentServerUrls && entity.contentServerUrls.length > 0
      ? entity.contentServerUrls[0]
      : 'https://peer.decentraland.org/content'

  const contentBaseUrl = `${contentServerUrl}/contents/`

  const report: ProcessReport = {
    entityId,
    entityType: 'scene',
    contentServerUrl,
    startedAt: new Date(),
    finishedAt: null,
    errors: [],
    godotLogs: [],
    godotProcessLogs: [],
    individualAssets: { total: 0, successful: 0, failed: 0 },
    result: null
  }

  const tempDir = path.join(process.cwd(), 'temp')
  try {
    await fs.mkdir(tempDir, { recursive: true })
  } catch {
    // Ignore if already exists
  }

  try {
    const timeoutMs = parseInt((await config.getString('ASSET_SERVER_TIMEOUT_MS')) ?? '600000', 10)

    // Single call to process the entire scene
    logger.info('Processing scene', { entityId, contentBaseUrl })

    let sceneResponse
    try {
      sceneResponse = await assetServer.processScene({
        sceneHash: entityId,
        contentBaseUrl
      })
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error)
      if (errorMsg.includes('No processable assets') || errorMsg.includes('400')) {
        logger.info('Scene has no processable assets', { entityId })
        report.finishedAt = new Date()
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }
      throw error
    }

    logger.info('Scene processing submitted', {
      entityId,
      batchId: sceneResponse.batch_id,
      totalAssets: sceneResponse.total_assets
    })

    // Wait for completion
    const batchResult = await assetServer.waitForCompletion(sceneResponse.batch_id, timeoutMs)

    // Collect job logs
    for (const job of batchResult.jobs || []) {
      if (job.error) {
        report.godotLogs.push(`[${job.hash}] ${job.status}: ${job.error}`)
      } else {
        report.godotLogs.push(`[${job.hash}] ${job.status} (${job.elapsed_secs}s)`)
      }
    }

    if (batchResult.status === 'failed') {
      const errorMsg = batchResult.error || ''
      if (errorMsg.includes('No assets completed successfully') || errorMsg.includes('No processable assets')) {
        logger.info('Scene has no processable assets (from batch result)', { entityId })
        report.finishedAt = new Date()
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }
      throw new Error(batchResult.error || 'Scene processing failed')
    }

    if (!batchResult.zip_path) {
      throw new Error('No main ZIP file created')
    }

    // Upload main ZIP
    const mainS3Key = `${entityId}-mobile.zip`
    await storage.storeFile(mainS3Key, batchResult.zip_path)
    await fs.rm(batchResult.zip_path, { force: true }).catch(() => {})
    logger.info('Uploaded main ZIP', { entityId, s3Key: mainS3Key })

    const individualZips: string[] = [mainS3Key]

    // Upload individual asset ZIPs
    const individualZipEntries = batchResult.individual_zips || []
    report.individualAssets.total = individualZipEntries.length

    for (const entry of individualZipEntries) {
      try {
        const s3Key = `${entry.hash}-mobile.zip`
        await storage.storeFile(s3Key, entry.zip_path)
        await fs.rm(entry.zip_path, { force: true }).catch(() => {})
        individualZips.push(s3Key)
        report.individualAssets.successful++
        report.godotLogs.push(`[${entry.hash}] uploaded successfully`)
      } catch (uploadError) {
        report.individualAssets.failed++
        const errorMsg = uploadError instanceof Error ? uploadError.message : String(uploadError)
        report.errors.push(`Failed to upload ZIP for ${entry.hash}: ${errorMsg}`)
        report.godotLogs.push(`[${entry.hash}] upload failed: ${errorMsg}`)
      }
    }

    logger.info('All ZIPs uploaded', {
      entityId,
      total: report.individualAssets.total,
      successful: report.individualAssets.successful,
      failed: report.individualAssets.failed
    })

    report.result = {
      success: report.individualAssets.failed === 0,
      batchId: sceneResponse.batch_id,
      optimizedAssets: report.individualAssets.successful,
      metadataZipPath: mainS3Key,
      individualZips
    }

    report.finishedAt = new Date()
    return report
  } catch (error) {
    logger.error(`Error processing scene ${entityId}`)
    logger.error(error as any)
    report.errors.push(error instanceof Error ? error.message : String(error))
    report.finishedAt = new Date()
    report.result = { success: false }
    return report
  }
}

/**
 * Process a wearable or emote entity - creates a single ZIP per GLTF
 */
async function processWearableOrEmote(
  entity: DeploymentToSqs,
  entityType: 'wearable' | 'emote',
  components: Pick<AppComponents, 'logs' | 'config' | 'storage' | 'assetServer' | 'fetch'>
): Promise<ProcessReport> {
  const { logs, storage, assetServer, config, fetch } = components
  const logger = logs.getLogger(`godot-optimizer:${entityType}`)

  const entityWithType = entity as DeploymentWithType
  const profileData = entityWithType._profileData

  // entityId is either the GLTF hash (from profile) or the entity hash
  const entityId = entity.entity.entityId
  const contentServerUrl =
    entity.contentServerUrls && entity.contentServerUrls.length > 0
      ? entity.contentServerUrls[0]
      : 'https://peer.decentraland.org/content'

  const contentBaseUrl = profileData?.contentBaseUrl || `${contentServerUrl}/contents/`

  const report: ProcessReport = {
    entityId,
    entityType,
    contentServerUrl,
    startedAt: new Date(),
    finishedAt: null,
    errors: [],
    godotLogs: [],
    godotProcessLogs: [],
    individualAssets: { total: 1, successful: 0, failed: 0 },
    result: null
  }

  const tempDir = path.join(process.cwd(), 'temp')
  try {
    await fs.mkdir(tempDir, { recursive: true })
  } catch {
    // Ignore if already exists
  }

  try {

    const timeoutMs = parseInt((await config.getString('ASSET_SERVER_TIMEOUT_MS')) ?? '600000', 10)

    let gltfHash: string
    let contentMapping: Record<string, string>

    if (profileData) {
      // Profile mode: entityId is already the GLTF hash, content mapping is provided
      gltfHash = entityId
      contentMapping = profileData.contentMapping
      logger.info('Using profile data', {
        gltfHash,
        gltfFile: profileData.gltfFile,
        originalEntityId: profileData.originalEntityId
      })
    } else {
      // Standard mode: fetch entity definition and find GLTFs
      logger.info('Fetching entity definition', { entityId, entityType })
      const entityDefinition = await fetchEntityDefinition(entityId, contentServerUrl, fetch)

      const gltfFiles = entityDefinition.content.filter(
        (c: { file: string }) => c.file.toLowerCase().endsWith('.glb') || c.file.toLowerCase().endsWith('.gltf')
      )

      if (gltfFiles.length === 0) {
        // No GLTF/GLB files - treat as success with 0 assets
        logger.info(`No GLTF/GLB files found in ${entityType}, skipping`, { entityId, entityType })
        report.finishedAt = new Date()
        report.individualAssets.total = 0
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }

      // Use first GLTF (for standard mode, we process the whole entity)
      gltfHash = gltfFiles[0].hash
      contentMapping = {}
      for (const content of entityDefinition.content) {
        contentMapping[content.file] = content.hash
      }

      logger.info('Found assets', {
        entityId,
        entityType,
        gltfs: gltfFiles.length
      })
    }

    // Pre-download content to Godot cache
    try {
      logger.info('Pre-downloading entity content', { entityId, entityType })
      const contentList = Object.entries(contentMapping).map(([file, hash]) => ({ file, hash }))
      const preloadResult = await preloadEntityContent(entityId, contentServerUrl, components, {
        contentMapping: contentList
      })
      logger.info('Pre-download complete', {
        entityId,
        entityType,
        downloaded: preloadResult.downloaded,
        failed: preloadResult.failed
      })
    } catch (preloadError) {
      logger.warn('Content pre-download failed, continuing with processing', {
        entityId,
        error: preloadError instanceof Error ? preloadError.message : String(preloadError)
      })
    }

    // 2. Build asset request
    const assetType: AssetType = entityType === 'wearable' ? 'wearable' : 'emote'
    const assets = [
      {
        url: `${contentBaseUrl}${gltfHash}`,
        type: assetType,
        hash: gltfHash,
        base_url: contentBaseUrl,
        content_mapping: contentMapping
      }
    ]

    // 3. Submit to asset server
    logger.info('Submitting asset for processing', {
      gltfHash,
      entityType
    })

    const response = await assetServer.processAssets({
      outputHash: gltfHash,
      assets
    })

    logger.info('Processing submitted', {
      gltfHash,
      batchId: response.batch_id
    })

    // 4. Wait for completion
    const result = await assetServer.waitForCompletion(response.batch_id, timeoutMs)

    // Collect job logs
    for (const job of result.jobs || []) {
      if (job.error) {
        report.godotLogs.push(`[${job.hash}] ${job.status}: ${job.error}`)
      } else {
        report.godotLogs.push(`[${job.hash}] ${job.status} (${job.elapsed_secs}s)`)
      }
    }

    if (result.status === 'failed') {
      // Log detailed job status for debugging
      const jobErrors = result.jobs
        ?.filter((j) => j.status === 'failed')
        .map((j) => `${j.job_id}: ${j.error || 'unknown'}`)
        .join('; ')
      logger.error('Asset processing failed', {
        gltfHash,
        batchId: response.batch_id,
        error: result.error || 'unknown',
        jobErrors: jobErrors || 'none'
      })

      // Check if this is "No assets completed successfully" - treat as success with 0 assets
      const errorMsg = result.error || ''
      if (errorMsg.includes('No assets completed successfully') || errorMsg.includes('No processable assets')) {
        logger.info(`${entityType} has no processable assets`, { entityId, gltfHash })
        report.finishedAt = new Date()
        report.individualAssets.total = 0
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }

      throw new Error(result.error || 'Processing failed')
    }

    if (!result.zip_path) {
      throw new Error('No ZIP file created')
    }

    // 5. Upload ZIP to storage
    const s3Key = `${gltfHash}-mobile.zip`
    await storage.storeFile(s3Key, result.zip_path)
    // Clean up temp file from asset-server
    await fs.rm(result.zip_path, { force: true }).catch(() => {})
    logger.info('Uploaded ZIP', { gltfHash, entityType, s3Key })

    report.individualAssets.successful = 1
    report.godotLogs.push(`[${gltfHash}] completed successfully`)
    report.result = {
      success: true,
      batchId: response.batch_id,
      optimizedAssets: 1,
      individualZips: [s3Key]
    }

    report.finishedAt = new Date()
    return report
  } catch (error) {
    logger.error(`Error processing ${entityType} ${entityId}`)
    logger.error(error as any)
    const errorMsg = error instanceof Error ? error.message : String(error)
    report.errors.push(errorMsg)
    report.godotLogs.push(`[${entityId}] exception: ${errorMsg}`)
    report.finishedAt = new Date()
    report.result = { success: false }
    report.individualAssets.failed = 1
    return report
  }
}

async function fetchEntityDefinition(
  entityId: string,
  contentServerUrl: string,
  fetch: AppComponents['fetch']
): Promise<{ id: string; content: Array<{ file: string; hash: string }> }> {
  const url = `${contentServerUrl}/contents/${entityId}`
  const response = await fetch.fetch(url)

  if (!response.ok) {
    // Consume body to free the connection
    await response.text().catch(() => {})
    throw new Error(`Failed to fetch entity definition: ${response.status}`)
  }

  return (await response.json()) as { id: string; content: Array<{ file: string; hash: string }> }
}

async function storeReport(
  report: ProcessReport,
  tempDir: string,
  storage: AppComponents['storage'],
  logger: ReturnType<AppComponents['logs']['getLogger']>
): Promise<void> {
  const reportPath = path.join(tempDir, `${report.entityId}-report.json`)
  try {
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2))

    const s3ReportKey = `${report.entityId}-report.json`
    await storage.storeFile(s3ReportKey, reportPath)
    logger.info('Stored report', { entityId: report.entityId, s3Key: s3ReportKey })
  } catch (reportError) {
    logger.error(`Failed to store report for ${report.entityId}`)
    logger.error(reportError as any)
  } finally {
    // Clean up temp report file
    await fs.rm(reportPath, { force: true }).catch(() => {})
  }
}
