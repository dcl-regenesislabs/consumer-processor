import fs from 'fs/promises'
import path from 'path'
import PQueue from 'p-queue'
import { AppComponents } from '../../types'

const DEFAULT_CACHE_PATH = path.join(
  process.env.HOME || '/root',
  '.local/share/godot/app_userdata/Decentraland/content'
)

const DEFAULT_CONCURRENCY = 10
const DEFAULT_TIMEOUT_MS = 30000

export async function clearGodotCache(
  logger: ReturnType<AppComponents['logs']['getLogger']>,
  cachePath?: string
): Promise<void> {
  const cacheDir = cachePath || process.env.GODOT_CONTENT_CACHE_PATH || DEFAULT_CACHE_PATH

  logger.info('Clearing Godot content cache', { cacheDir })

  try {
    // Remove and recreate the directory
    await fs.rm(cacheDir, { recursive: true, force: true })
    await fs.mkdir(cacheDir, { recursive: true })
    logger.info('Cache cleared successfully')
  } catch (error) {
    logger.warn('Failed to clear cache, creating fresh directory', {
      error: error instanceof Error ? error.message : String(error)
    })
    await fs.mkdir(cacheDir, { recursive: true })
  }
}

export async function fetchEntityContent(
  entityId: string,
  contentServerUrl: string,
  fetch: AppComponents['fetch']
): Promise<Array<{ file: string; hash: string }>> {
  const url = `${contentServerUrl}/entities/active`
  const response = await fetch.fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids: [entityId] })
  })

  if (!response.ok) {
    throw new Error(`Failed to fetch entity: ${response.status}`)
  }

  const entities = (await response.json()) as Array<{ id: string; content: Array<{ file: string; hash: string }> }>
  const entity = entities.find((e) => e.id === entityId)

  if (!entity) {
    throw new Error(`Entity ${entityId} not found`)
  }

  return entity.content
}

export async function downloadToCache(
  hash: string,
  contentBaseUrl: string,
  cachePath: string,
  fetch: AppComponents['fetch'],
  logger: ReturnType<AppComponents['logs']['getLogger']>,
  timeoutMs: number
): Promise<boolean> {
  const filePath = path.join(cachePath, hash)
  const url = `${contentBaseUrl}${hash}`

  try {
    // Check if already exists
    try {
      await fs.access(filePath)
      return true // Already downloaded
    } catch {
      // File doesn't exist, proceed with download
    }

    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)

    try {
      const response = await fetch.fetch(url, { signal: controller.signal })

      if (!response.ok) {
        logger.warn('Failed to download content', { hash, status: response.status })
        return false
      }

      const buffer = Buffer.from(await response.arrayBuffer())
      await fs.writeFile(filePath, buffer)
      return true
    } finally {
      clearTimeout(timeout)
    }
  } catch (error) {
    logger.warn('Error downloading content', {
      hash,
      error: error instanceof Error ? error.message : String(error)
    })
    return false
  }
}

export async function preloadEntityContent(
  entityId: string,
  contentServerUrl: string,
  components: Pick<AppComponents, 'logs' | 'fetch' | 'config'>,
  options?: {
    contentMapping?: Array<{ file: string; hash: string }>
    skipClear?: boolean
  }
): Promise<{ total: number; downloaded: number; failed: number }> {
  const { logs, fetch, config } = components
  const logger = logs.getLogger('content-preloader')

  const cachePath = (await config.getString('GODOT_CONTENT_CACHE_PATH')) || DEFAULT_CACHE_PATH
  const concurrency = parseInt(
    (await config.getString('CONTENT_DOWNLOAD_CONCURRENCY')) || String(DEFAULT_CONCURRENCY),
    10
  )
  const timeoutMs = parseInt((await config.getString('CONTENT_DOWNLOAD_TIMEOUT_MS')) || String(DEFAULT_TIMEOUT_MS), 10)
  const contentBaseUrl = `${contentServerUrl}/contents/`

  // Clear cache before downloading (unless explicitly skipped)
  if (!options?.skipClear) {
    await clearGodotCache(logger, cachePath)
  }

  // Get content list
  let contentList: Array<{ file: string; hash: string }>
  if (options?.contentMapping) {
    contentList = options.contentMapping
  } else {
    contentList = await fetchEntityContent(entityId, contentServerUrl, fetch)
  }

  const uniqueHashes = [...new Set(contentList.map((c) => c.hash))]

  logger.info('Pre-downloading entity content', {
    entityId,
    totalFiles: contentList.length,
    uniqueHashes: uniqueHashes.length,
    concurrency
  })

  const result = { total: uniqueHashes.length, downloaded: 0, failed: 0 }

  const queue = new PQueue({ concurrency })

  const downloadPromises = uniqueHashes.map((hash) =>
    queue.add(async () => {
      const success = await downloadToCache(hash, contentBaseUrl, cachePath, fetch, logger, timeoutMs)
      if (success) {
        result.downloaded++
      } else {
        result.failed++
      }
    })
  )

  await Promise.all(downloadPromises)

  logger.info('Pre-download completed', {
    entityId,
    total: result.total,
    downloaded: result.downloaded,
    failed: result.failed
  })

  return result
}
