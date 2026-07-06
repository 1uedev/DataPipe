// Live debugging state (Increment 5, DBG-100/110/120/170): one WebSocket
// subscription per open flow, fed into per-node ring buffers (DBG-100), a
// global sidebar event list (DBG-110), and wire pulse/rate data (DBG-120).
import { create } from 'zustand'
import { DebugSocket } from '../api/debugSocket'
import type { DebugEvent, DebugWSMessage, WireMetrics } from '../api/types'

const NODE_RING_BUFFER_SIZE = 50
const SIDEBAR_BUFFER_SIZE = 200
const PULSE_MS = 400

// A stable reference for "no events yet" selectors (e.g.
// `nodeEvents[nodeId] ?? EMPTY_EVENTS`) — a fresh `[]` literal on every call
// would make useSyncExternalStore think the snapshot changes every render.
export const EMPTY_EVENTS: DebugEvent[] = []

export function sourceWireKey(nodeId: string, port: string): string {
  return `${nodeId}:${port}`
}

export function wireKey(fromNode: string, fromPort: string, toNode: string, toPort: string): string {
  return `${sourceWireKey(fromNode, fromPort)}->${sourceWireKey(toNode, toPort)}`
}

interface DebugState {
  flowId: string | null
  connected: boolean
  nodeEvents: Record<string, DebugEvent[]>
  sidebarEvents: DebugEvent[]
  sidebarPaused: boolean
  wireMetrics: Record<string, WireMetrics>
  pulsingSourceWires: Record<string, number> // sourceWireKey -> pulse generation counter

  connect: (flowId: string) => void
  disconnect: () => void
  clearSidebar: () => void
  toggleSidebarPaused: () => void
}

let socket: DebugSocket | null = null

export const useDebugStore = create<DebugState>((set, get) => {
  function handleMessage(msg: DebugWSMessage) {
    if (msg.type === 'event') {
      const e = msg.event
      if (e.direction === 'sidebar') {
        if (get().sidebarPaused) return
        set((s) => {
          if (s.sidebarEvents.some((x) => x.id === e.id)) return {} // e.g. a brief overlap between an old closing socket and a new one
          return { sidebarEvents: [...s.sidebarEvents, e].slice(-SIDEBAR_BUFFER_SIZE) }
        })
        return
      }

      set((s) => {
        const existing = s.nodeEvents[e.nodeId] ?? []
        if (existing.some((x) => x.id === e.id)) return {} // same overlap case as sidebar events above
        const nodeEvents = { ...s.nodeEvents, [e.nodeId]: [...existing, e].slice(-NODE_RING_BUFFER_SIZE) }
        if (e.direction !== 'out') return { nodeEvents }

        const key = sourceWireKey(e.nodeId, e.port)
        const pulsingSourceWires = { ...s.pulsingSourceWires, [key]: (s.pulsingSourceWires[key] ?? 0) + 1 }
        return { nodeEvents, pulsingSourceWires }
      })

      if (e.direction === 'out') {
        const key = sourceWireKey(e.nodeId, e.port)
        const gen = get().pulsingSourceWires[key]
        setTimeout(() => {
          set((s) => {
            if (s.pulsingSourceWires[key] !== gen) return {} // a newer pulse already superseded this one
            const rest = { ...s.pulsingSourceWires }
            delete rest[key]
            return { pulsingSourceWires: rest }
          })
        }, PULSE_MS)
      }
      return
    }

    const m = msg.metrics
    set((s) => ({ wireMetrics: { ...s.wireMetrics, [wireKey(m.fromNode, m.fromPort, m.toNode, m.toPort)]: m } }))
  }

  return {
    flowId: null,
    connected: false,
    nodeEvents: {},
    sidebarEvents: [],
    sidebarPaused: false,
    wireMetrics: {},
    pulsingSourceWires: {},

    connect: (flowId: string) => {
      if (get().flowId === flowId && socket) return
      socket?.close()
      socket = new DebugSocket(flowId, handleMessage)
      set({
        flowId,
        connected: true,
        nodeEvents: {},
        sidebarEvents: [],
        wireMetrics: {},
        pulsingSourceWires: {},
      })
    },

    disconnect: () => {
      socket?.close()
      socket = null
      set({ flowId: null, connected: false })
    },

    clearSidebar: () => set({ sidebarEvents: [] }),
    toggleSidebarPaused: () => set((s) => ({ sidebarPaused: !s.sidebarPaused })),
  }
})
