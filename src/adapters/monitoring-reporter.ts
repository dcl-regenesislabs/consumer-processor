import type { ILoggerComponent, IConfigComponent, IBaseComponent } from '@well-known-components/interfaces'
import type { IFetchComponent } from '@well-known-components/http-server'
import crypto from 'crypto'

export interface IMonitoringReporter extends IBaseComponent {
  reportHeartbeat(data: HeartbeatData): void
  reportJobComplete(data: JobCompleteData): void
  getConsumerId(): string
}

export interface HeartbeatData {
  status: 'idle' | 'processing'
  currentSceneId?: string
  currentStep?: string
  progressPercent?: number
  startedAt?: string
  isPriority?: boolean
  entityType?: 'scene' | 'wearable' | 'emote'
}

export interface JobCompleteData {
  sceneId: string
  status: 'success' | 'failed'
  startedAt: string
  completedAt: string
  durationMs: number
  errorMessage?: string
  isPriority?: boolean
  entityType?: 'scene' | 'wearable' | 'emote'
  thumbnailUrl?: string
  reportJson?: object // Full processing report to store in DB (avoids CDN cache)
}

interface MonitoringReporterComponents {
  logs: ILoggerComponent
  config: IConfigComponent
  fetch: IFetchComponent
}

export function createMonitoringReporter(
  components: MonitoringReporterComponents,
  processMethod: string
): IMonitoringReporter {
  const { logs, config, fetch } = components
  const logger = logs.getLogger('monitoring-reporter')

  const consumerId = crypto.randomUUID()
  let monitoringUrl: string | undefined
  let monitoringSecret: string | undefined
  let heartbeatInterval: NodeJS.Timeout | undefined
  let currentHeartbeatData: HeartbeatData = { status: 'idle' }
  let isRunning = false

  async function initConfig() {
    monitoringUrl = await config.getString('MONITORING_URL')
    monitoringSecret = await config.getString('MONITORING_SECRET')

    if (!monitoringUrl || !monitoringSecret) {
      logger.info('Monitoring not configured (MONITORING_URL or MONITORING_SECRET missing)')
    } else {
      logger.info('Monitoring configured', { consumerId, monitoringUrl })
    }
  }

  async function report(endpoint: string, data: object): Promise<void> {
    if (!monitoringUrl || !monitoringSecret) {
      return
    }

    let controller: AbortController | null = new AbortController()
    const timeoutId = setTimeout(() => controller?.abort(), 5000)

    try {
      const url = `${monitoringUrl}${endpoint}`

      const response = await fetch.fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...data, secret: monitoringSecret }),
        signal: controller.signal
      })
      // Consume response body to free the connection
      await response.text().catch(() => {})
    } catch (error) {
      // Silently ignore - monitoring should never block pipeline
      logger.debug('Monitoring report failed (non-blocking)', {
        error: error instanceof Error ? error.message : 'Unknown error'
      })
    } finally {
      clearTimeout(timeoutId)
      controller = null
    }
  }

  function sendHeartbeat() {
    report('/api/monitoring/heartbeat', {
      consumerId,
      processMethod,
      ...currentHeartbeatData
    })
  }

  function startHeartbeat() {
    if (heartbeatInterval) {
      return
    }

    // Send initial heartbeat
    sendHeartbeat()

    // Set up interval (every 10 seconds)
    heartbeatInterval = setInterval(sendHeartbeat, 10000)
  }

  function stopHeartbeat() {
    if (heartbeatInterval) {
      clearInterval(heartbeatInterval)
      heartbeatInterval = undefined
    }
  }

  return {
    async start() {
      await initConfig()
      isRunning = true
      startHeartbeat()
    },

    async stop() {
      isRunning = false
      stopHeartbeat()
    },

    getConsumerId() {
      return consumerId
    },

    reportHeartbeat(data: HeartbeatData) {
      currentHeartbeatData = data
      // Heartbeat will be sent on next interval, but also send immediately for status changes
      if (isRunning) {
        sendHeartbeat()
      }
    },

    reportJobComplete(data: JobCompleteData) {
      report('/api/monitoring/job-complete', {
        consumerId,
        processMethod,
        ...data
      })

      // Reset heartbeat data to idle
      currentHeartbeatData = { status: 'idle' }
    }
  }
}
