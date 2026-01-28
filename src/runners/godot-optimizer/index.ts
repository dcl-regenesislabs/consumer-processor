import { DeploymentToSqs } from '@dcl/schemas/dist/misc/deployments-to-sqs'
import fs from 'fs/promises'
import path from 'path'
import { TaskQueueMessage } from '../../adapters/sqs'
import { AppComponents } from '../../types'
import { AssetType } from '../../adapters/asset-server'
import AdmZip from 'adm-zip'
import { preloadEntityContent } from './content-preloader'

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

type SceneMetadata = {
  optimizedContent: string[]
  externalSceneDependencies: Record<string, string[]>
  originalSizes?: Record<string, number[]>
  hashSizeMap?: Record<string, number>
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
    // Get Godot logs before restart (they will be cleared on restart)
    const godotProcessLogs = assetServer.getGodotLogs()
    if (godotProcessLogs.length > 0) {
      logger.info('Captured Godot process logs', { lineCount: godotProcessLogs.length })
    }

    // Add Godot process logs to report
    if (report) {
      report.godotLogs = [...report.godotLogs, ...godotProcessLogs]
      // Store the report with Godot logs included
      await storeReport(report, tempDir, storage, logger)
    }

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
    individualAssets: { total: 0, successful: 0, failed: 0 },
    result: null
  }

  const tempDir = path.join(process.cwd(), 'temp')
  try {
    await fs.mkdir(tempDir, { recursive: true })
  } catch {
    // Ignore if already exists
  }

  // Pre-download content to Godot cache
  try {
    logger.info('Pre-downloading scene content', { entityId })
    const preloadResult = await preloadEntityContent(entityId, contentServerUrl, components)
    logger.info('Pre-download complete', {
      entityId,
      downloaded: preloadResult.downloaded,
      failed: preloadResult.failed
    })
  } catch (preloadError) {
    logger.warn('Content pre-download failed, continuing with processing', {
      entityId,
      error: preloadError instanceof Error ? preloadError.message : String(preloadError)
    })
  }

  try {
    // 1. Check if asset-server is ready
    const isReady = await assetServer.isReady()
    if (!isReady) {
      throw new Error('Asset-server is not ready')
    }

    const timeoutMs = parseInt((await config.getString('ASSET_SERVER_TIMEOUT_MS')) ?? '600000', 10)
    const concurrentBundles = parseInt((await config.getString('ASSET_SERVER_CONCURRENT_BUNDLES')) ?? '4', 10)

    // 2. Process scene with pack_hashes=[] to get metadata only
    logger.info('Processing scene for metadata', { entityId, contentBaseUrl })

    let metadataResponse
    try {
      metadataResponse = await assetServer.processScene({
        sceneHash: entityId,
        contentBaseUrl,
        outputHash: entityId,
        packHashes: [] // Empty array = metadata only
      })
    } catch (error) {
      // Check if this is a "no processable assets" error - treat as success
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

    logger.info('Metadata processing submitted', {
      entityId,
      batchId: metadataResponse.batch_id,
      totalAssets: metadataResponse.total_assets
    })

    // 3. Wait for metadata processing to complete
    const metadataResult = await assetServer.waitForCompletion(metadataResponse.batch_id, timeoutMs)

    // Collect job logs from metadata result
    for (const job of metadataResult.jobs || []) {
      if (job.error) {
        report.godotLogs.push(`[${job.hash}] ${job.status}: ${job.error}`)
      } else {
        report.godotLogs.push(`[${job.hash}] ${job.status} (${job.elapsed_secs}s)`)
      }
    }

    if (metadataResult.status === 'failed') {
      // Check if this is "No assets completed successfully" - treat as success with 0 assets
      const errorMsg = metadataResult.error || ''
      if (errorMsg.includes('No assets completed successfully') || errorMsg.includes('No processable assets')) {
        logger.info('Scene has no processable assets (from metadata result)', { entityId })
        report.finishedAt = new Date()
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }
      throw new Error(metadataResult.error || 'Metadata processing failed')
    }

    if (!metadataResult.zip_path) {
      throw new Error('No metadata ZIP file created')
    }

    logger.info('Metadata processing completed', {
      entityId,
      zipPath: metadataResult.zip_path
    })

    // 4. Extract metadata from ZIP to get all asset hashes
    const metadataExtraction = extractMetadataFromZip(metadataResult.zip_path, entityId)

    // Handle empty scenes (no GLTF/images to process)
    if (!metadataExtraction.success) {
      if (metadataExtraction.reason === 'empty_zip' || metadataExtraction.reason === 'metadata_not_found') {
        // This is likely a scene with no optimizable assets - treat as success
        logger.info('Scene has no optimizable assets', {
          entityId,
          reason: metadataExtraction.reason,
          zipEntries: metadataExtraction.entries.join(', ') || '(empty)'
        })

        report.finishedAt = new Date()
        report.result = {
          success: true,
          optimizedAssets: 0,
          individualZips: []
        }
        return report
      }

      // Parse error is a real failure
      throw new Error(
        `Could not extract metadata from ZIP: ${metadataExtraction.reason} - ${metadataExtraction.error || 'unknown error'}`
      )
    }

    const metadata = metadataExtraction.metadata
    const gltfHashes = new Set(Object.keys(metadata.externalSceneDependencies || {}))
    const allHashes = new Set(metadata.optimizedContent || [])
    const textureHashes = new Set([...allHashes].filter((h) => !gltfHashes.has(h)))

    const allAssetHashes = [...gltfHashes, ...textureHashes]
    report.individualAssets.total = allAssetHashes.length

    // Handle scenes where metadata exists but has no assets
    if (allAssetHashes.length === 0) {
      logger.info('Scene metadata exists but has no assets to process', { entityId })

      // Still upload the metadata ZIP even if empty
      const metadataS3Key = `${entityId}-mobile.zip`
      await storage.storeFile(metadataS3Key, metadataResult.zip_path)
      // Clean up temp file from asset-server
      await fs.rm(metadataResult.zip_path, { force: true }).catch(() => {})

      report.finishedAt = new Date()
      report.result = {
        success: true,
        optimizedAssets: 0,
        metadataZipPath: metadataS3Key,
        individualZips: [metadataS3Key]
      }
      return report
    }

    logger.info('Found assets to bundle individually', {
      entityId,
      gltfs: gltfHashes.size,
      textures: textureHashes.size,
      total: allAssetHashes.length
    })

    // 5. Upload metadata ZIP to storage
    const metadataS3Key = `${entityId}-mobile.zip`
    await storage.storeFile(metadataS3Key, metadataResult.zip_path)
    // Clean up temp file from asset-server
    await fs.rm(metadataResult.zip_path, { force: true }).catch(() => {})
    logger.info('Uploaded metadata ZIP', { entityId, s3Key: metadataS3Key })

    // 6. Process each asset individually (with concurrency limit)
    const individualZips: string[] = [metadataS3Key]

    // Process in batches to limit concurrency
    for (let i = 0; i < allAssetHashes.length; i += concurrentBundles) {
      const batch = allAssetHashes.slice(i, i + concurrentBundles)

      logger.info('Processing asset batch', {
        entityId,
        batchStart: i,
        batchSize: batch.length,
        totalAssets: allAssetHashes.length
      })

      // Submit all in this batch concurrently
      const batchPromises = batch.map(async (assetHash) => {
        try {
          const response = await assetServer.processScene({
            sceneHash: entityId,
            contentBaseUrl,
            outputHash: assetHash,
            packHashes: [assetHash]
          })

          const result = await assetServer.waitForCompletion(response.batch_id, timeoutMs)

          // Collect job logs
          for (const job of result.jobs || []) {
            if (job.error) {
              report.godotLogs.push(`[${assetHash}] ${job.status}: ${job.error}`)
            }
          }

          if (result.status === 'completed' && result.zip_path) {
            const s3Key = `${assetHash}-mobile.zip`
            await storage.storeFile(s3Key, result.zip_path)
            // Clean up temp file from asset-server
            await fs.rm(result.zip_path, { force: true }).catch(() => {})
            report.individualAssets.successful++
            individualZips.push(s3Key)
            report.godotLogs.push(`[${assetHash}] completed successfully`)
            return { success: true, hash: assetHash, s3Key }
          } else {
            report.individualAssets.failed++
            const errorMsg = result.error || 'Unknown error'
            report.errors.push(`Asset ${assetHash} failed: ${errorMsg}`)
            report.godotLogs.push(`[${assetHash}] failed: ${errorMsg}`)
            return { success: false, hash: assetHash, error: result.error }
          }
        } catch (error) {
          report.individualAssets.failed++
          const errorMsg = error instanceof Error ? error.message : String(error)
          report.errors.push(`Asset ${assetHash} failed: ${errorMsg}`)
          report.godotLogs.push(`[${assetHash}] exception: ${errorMsg}`)
          return { success: false, hash: assetHash, error: errorMsg }
        }
      })

      const results = await Promise.all(batchPromises)

      const successful = results.filter((r) => r.success).length
      const failed = results.filter((r) => !r.success).length
      logger.info('Batch completed', {
        entityId,
        batchStart: i,
        successful,
        failed
      })
    }

    logger.info('All individual assets processed', {
      entityId,
      total: report.individualAssets.total,
      successful: report.individualAssets.successful,
      failed: report.individualAssets.failed
    })

    report.result = {
      success: report.individualAssets.failed === 0,
      batchId: metadataResponse.batch_id,
      optimizedAssets: report.individualAssets.successful,
      metadataZipPath: metadataS3Key,
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
    // 1. Check if asset-server is ready
    const isReady = await assetServer.isReady()
    if (!isReady) {
      throw new Error('Asset-server is not ready')
    }

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

type MetadataExtractionResult =
  | { success: true; metadata: SceneMetadata }
  | { success: false; reason: 'empty_zip' | 'metadata_not_found' | 'parse_error'; entries: string[]; error?: string }

function extractMetadataFromZip(zipPath: string, sceneHash: string): MetadataExtractionResult {
  let zip: AdmZip | null = null
  try {
    zip = new AdmZip(zipPath)
    const entries = zip.getEntries().map((e) => e.entryName)
    const metadataFilename = `${sceneHash}-optimized.json`
    const entry = zip.getEntry(metadataFilename)

    if (entries.length === 0) {
      return { success: false, reason: 'empty_zip', entries }
    }

    if (!entry) {
      return { success: false, reason: 'metadata_not_found', entries }
    }

    const content = zip.readAsText(entry)
    const metadata = JSON.parse(content) as SceneMetadata
    return { success: true, metadata }
  } catch (error) {
    return {
      success: false,
      reason: 'parse_error',
      entries: [],
      error: error instanceof Error ? error.message : String(error)
    }
  } finally {
    // Help garbage collector by explicitly dereferencing
    zip = null
  }
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
