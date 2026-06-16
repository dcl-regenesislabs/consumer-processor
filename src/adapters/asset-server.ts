import { IFetchComponent } from '@well-known-components/http-server'
import { ILoggerComponent } from '@well-known-components/interfaces'
import { exec } from 'child_process'
import { promisify } from 'util'

const execAsync = promisify(exec)

export type AssetType = 'scene' | 'wearable' | 'emote' | 'texture'

export interface IAssetServerComponent {
  isReady(): Promise<boolean>
  processScene(params: ProcessSceneParams): Promise<ProcessSceneResponse>
  processAssets(params: ProcessAssetsParams): Promise<ProcessAssetsResponse>
  getBatchStatus(batchId: string): Promise<BatchStatus>
  waitForCompletion(batchId: string, timeoutMs?: number): Promise<BatchStatus>
  restartGodot(): Promise<boolean>
  getGodotLogs(): string[]
  clearGodotLogs(): void
}

export type ProcessSceneParams = {
  sceneHash: string
  contentBaseUrl: string
  outputHash?: string
}

export type ProcessSceneResponse = {
  batch_id: string
  output_hash: string
  scene_hash: string
  total_assets: number
  jobs: Array<{ job_id: string; hash: string; status: string }>
}

export type AssetRequest = {
  url: string
  type: AssetType
  hash: string
  base_url: string
  content_mapping?: Record<string, string>
}

export type ProcessAssetsParams = {
  outputHash?: string
  assets: AssetRequest[]
}

export type ProcessAssetsResponse = {
  batch_id: string
  output_hash: string
  jobs: Array<{
    job_id: string
    hash: string
    status: string
  }>
  total: number
}

export type BatchStatus = {
  batch_id: string
  output_hash: string
  status: 'processing' | 'packing' | 'completed' | 'failed'
  progress: number
  zip_path?: string
  error?: string
  jobs: JobStatus[]
  individual_zips?: Array<{ hash: string; zip_path: string }>
}

export type JobStatus = {
  job_id: string
  hash: string
  asset_type: AssetType
  status: 'queued' | 'downloading' | 'processing' | 'completed' | 'failed'
  progress: number
  elapsed_secs: number
  error?: string
}

export type AssetServerConfig = {
  baseUrl: string
}

export function createAssetServerComponent(
  components: { logs: ILoggerComponent; fetch: IFetchComponent },
  config: AssetServerConfig
): IAssetServerComponent {
  const { logs, fetch } = components
  const { baseUrl } = config
  const logger = logs.getLogger('asset-server')

  // Buffer to store Godot process output
  let godotLogs: string[] = []
  const MAX_LOG_LINES = 5000 // Limit to prevent memory issues

  function addGodotLog(line: string) {
    if (line.trim()) {
      godotLogs.push(line.trim())
      // Keep only the last MAX_LOG_LINES
      if (godotLogs.length > MAX_LOG_LINES) {
        godotLogs = godotLogs.slice(-MAX_LOG_LINES)
      }
    }
  }

  function getGodotLogs(): string[] {
    return [...godotLogs]
  }

  function clearGodotLogs(): void {
    godotLogs = []
  }

  async function isReady(): Promise<boolean> {
    try {
      const response = await fetch.fetch(`${baseUrl}/health`)
      // Consume response body to free the connection
      await response.text().catch(() => {})
      return response.ok
    } catch {
      return false
    }
  }

  async function processScene(params: ProcessSceneParams): Promise<ProcessSceneResponse> {
    const body: Record<string, unknown> = {
      scene_hash: params.sceneHash,
      content_base_url: params.contentBaseUrl,
    }
    if (params.outputHash) body.output_hash = params.outputHash

    logger.info('Submitting scene for processing', { sceneHash: params.sceneHash, baseUrl })

    const response = await fetch.fetch(`${baseUrl}/process-scene`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    })

    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to submit scene for processing: ${response.status} ${errorText}`)
    }

    return (await response.json()) as ProcessSceneResponse
  }

  async function processAssets(params: ProcessAssetsParams): Promise<ProcessAssetsResponse> {
    const body = {
      output_hash: params.outputHash,
      assets: params.assets
    }

    logger.info('Submitting assets for processing', {
      outputHash: params.outputHash || 'auto',
      assetCount: params.assets.length,
      baseUrl
    })

    const response = await fetch.fetch(`${baseUrl}/process`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    })

    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to submit assets for processing: ${response.status} ${errorText}`)
    }

    return (await response.json()) as ProcessAssetsResponse
  }

  async function getBatchStatus(batchId: string): Promise<BatchStatus> {
    const response = await fetch.fetch(`${baseUrl}/status/${batchId}`)

    if (!response.ok) {
      const errorText = await response.text()
      throw new Error(`Failed to get batch status: ${response.status} ${errorText}`)
    }

    return (await response.json()) as BatchStatus
  }

  async function waitForCompletion(batchId: string, timeoutMs: number = 600000): Promise<BatchStatus> {
    const startTime = Date.now()
    const pollIntervalMs = 2000

    while (Date.now() - startTime < timeoutMs) {
      const status = await getBatchStatus(batchId)

      logger.debug('Batch status', {
        batchId,
        status: status.status,
        progress: status.progress
      })

      if (status.status === 'completed' || status.status === 'failed') {
        return status
      }

      // Use a cancellable sleep pattern
      await new Promise<void>((resolve) => {
        const timeoutId = setTimeout(() => resolve(), pollIntervalMs)
        // Ensure timeout is unreferenced so it doesn't keep the process alive
        if (timeoutId.unref) {
          timeoutId.unref()
        }
      })
    }

    throw new Error(`Timeout waiting for batch ${batchId} to complete after ${timeoutMs}ms`)
  }

  async function restartGodot(): Promise<boolean> {
    const port = process.env.ASSET_SERVER_PORT || '8080'
    logger.info('Restarting Godot asset-server...')

    try {
      // Kill existing Godot process - use simpler pattern
      try {
        await execAsync('pkill -9 -f "decentraland.godot.client" || true')
        logger.info('Sent kill signal to Godot process')
      } catch {
        // Ignore errors - process might not exist
      }

      // Wait for process to fully terminate
      await new Promise((resolve) => setTimeout(resolve, 2000))

      // Clear logs from previous run
      clearGodotLogs()
      addGodotLog(`[${new Date().toISOString()}] Starting Godot asset-server...`)

      // Start new Godot process in background.
      // NO --headless: that wires up the dummy RenderingServer, so the
      // impostor-bake SubViewport readback returns null pixels and every
      // octahedral atlas comes back as `fail_blank_albedo` in
      // [impostor-bake] requested=N baked=0. We need opengl3 + Xvfb (set
      // up in the container entrypoint with Mesa llvmpipe) so SubViewport
      // actually rasterizes.
      const godotCmd = `/app/decentraland.godot.client.x86_64 --rendering-driver opengl3 --audio-driver Dummy --asset-server --asset-server-port ${port}`
      const child = exec(godotCmd)

      // Capture stdout
      if (child.stdout) {
        child.stdout.on('data', (data: Buffer | string) => {
          const lines = data.toString().split('\n')
          for (const line of lines) {
            addGodotLog(line)
          }
        })
      }

      // Capture stderr
      if (child.stderr) {
        child.stderr.on('data', (data: Buffer | string) => {
          const lines = data.toString().split('\n')
          for (const line of lines) {
            addGodotLog(`[stderr] ${line}`)
          }
        })
      }

      // Log when process exits
      child.on('exit', (code, signal) => {
        addGodotLog(`[${new Date().toISOString()}] Godot process exited with code ${code}, signal ${signal}`)
      })

      // Detach from child process - we don't need to track it
      child.unref()

      // Wait for server to be ready (up to 60 seconds)
      for (let i = 0; i < 60; i++) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
        if (await isReady()) {
          logger.info('Godot asset-server is ready after restart')
          addGodotLog(`[${new Date().toISOString()}] Godot asset-server is ready`)
          return true
        }
      }

      logger.error('Godot asset-server failed to become ready after restart')
      addGodotLog(`[${new Date().toISOString()}] ERROR: Godot asset-server failed to become ready`)
      return false
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error)
      logger.error('Failed to restart Godot', { error: errorMsg })
      addGodotLog(`[${new Date().toISOString()}] ERROR: Failed to restart Godot: ${errorMsg}`)
      return false
    }
  }

  return {
    isReady,
    processScene,
    processAssets,
    getBatchStatus,
    waitForCompletion,
    restartGodot,
    getGodotLogs,
    clearGodotLogs
  }
}
