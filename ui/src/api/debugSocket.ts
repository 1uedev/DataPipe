// Live debugging WebSocket client (docs/api/debug-websocket.md, DBG-100/
// 110/120/170). One socket per subscribed flow; reconnects with backoff so
// a transient control-plane restart doesn't require reopening the
// inspector by hand.
import { getToken } from './token'
import type { DebugWSMessage } from './types'

const BASE_PATH = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'
const MIN_RETRY_MS = 500
const MAX_RETRY_MS = 10_000

function wsURL(flowId: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const token = getToken() ?? ''
  return `${proto}//${window.location.host}${BASE_PATH}/ws/debug?flowId=${encodeURIComponent(flowId)}&token=${encodeURIComponent(token)}`
}

export class DebugSocket {
  private readonly flowId: string
  private readonly onMessage: (msg: DebugWSMessage) => void
  private ws: WebSocket | null = null
  private closedByCaller = false
  private retryMs = MIN_RETRY_MS
  private retryTimer: ReturnType<typeof setTimeout> | null = null

  constructor(flowId: string, onMessage: (msg: DebugWSMessage) => void) {
    this.flowId = flowId
    this.onMessage = onMessage
    this.connect()
  }

  private connect(): void {
    if (this.closedByCaller) return
    const ws = new WebSocket(wsURL(this.flowId))
    this.ws = ws

    ws.onopen = () => {
      this.retryMs = MIN_RETRY_MS
    }
    ws.onmessage = (ev: MessageEvent<string>) => {
      try {
        this.onMessage(JSON.parse(ev.data) as DebugWSMessage)
      } catch {
        // Malformed frame: ignore rather than tear down the whole socket.
      }
    }
    ws.onclose = () => {
      if (this.closedByCaller) return
      this.retryTimer = setTimeout(() => this.connect(), this.retryMs)
      this.retryMs = Math.min(this.retryMs * 2, MAX_RETRY_MS)
    }
    ws.onerror = () => ws.close()
  }

  close(): void {
    this.closedByCaller = true
    if (this.retryTimer) clearTimeout(this.retryTimer)
    this.ws?.close()
  }
}
