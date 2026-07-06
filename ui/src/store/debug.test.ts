import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { DebugEvent, DebugWSMessage } from '../api/types'

interface FakeSocket {
  flowId: string
  onMessage: (msg: DebugWSMessage) => void
  closed: boolean
}

const instances: FakeSocket[] = []

vi.mock('../api/debugSocket', () => {
  class DebugSocket {
    flowId: string
    onMessage: (msg: DebugWSMessage) => void
    closed = false
    constructor(flowId: string, onMessage: (msg: DebugWSMessage) => void) {
      this.flowId = flowId
      this.onMessage = onMessage
      instances.push(this)
    }
    close() {
      this.closed = true
    }
  }
  return { DebugSocket }
})

// Imported after the mock so useDebugStore's `new DebugSocket(...)` resolves
// to the fake above.
const { useDebugStore, sourceWireKey, wireKey } = await import('./debug')

function lastSocket(): FakeSocket {
  const s = instances[instances.length - 1]
  if (!s) throw new Error('no DebugSocket constructed yet')
  return s
}

function nodeEvent(overrides: Partial<DebugEvent> = {}): DebugWSMessage {
  const event: DebugEvent = {
    id: 'e1',
    flowId: 'flow-1',
    nodeId: 'n1',
    port: 'out',
    direction: 'out',
    label: '',
    timeUnixMs: 1,
    datagramId: 'd1',
    correlationId: 'd1',
    causationId: '',
    quality: 'GOOD',
    valueJson: '1',
    truncated: false,
    fullLength: 0,
    ...overrides,
  }
  return { type: 'event', event }
}

describe('debug store', () => {
  beforeEach(() => {
    instances.length = 0
    useDebugStore.getState().disconnect()
  })
  afterEach(() => {
    useDebugStore.getState().disconnect()
    vi.useRealTimers()
  })

  it('connect opens a DebugSocket for the flow and resets state from a prior flow', () => {
    useDebugStore.getState().connect('flow-1')
    expect(lastSocket().flowId).toBe('flow-1')
    expect(useDebugStore.getState().connected).toBe(true)

    lastSocket().onMessage(nodeEvent())
    expect(useDebugStore.getState().nodeEvents['n1']).toHaveLength(1)

    useDebugStore.getState().connect('flow-2')
    expect(instances[0].closed).toBe(true)
    expect(useDebugStore.getState().nodeEvents).toEqual({})
  })

  it('routes in/out events into the node ring buffer, capped at 50', () => {
    useDebugStore.getState().connect('flow-1')
    const socket = lastSocket()
    for (let i = 0; i < 60; i++) {
      socket.onMessage(nodeEvent({ id: `e${i}`, direction: i % 2 === 0 ? 'in' : 'out' }))
    }
    const events = useDebugStore.getState().nodeEvents['n1']
    expect(events).toHaveLength(50)
    expect(events[0].id).toBe('e10') // oldest 10 evicted
    expect(events[49].id).toBe('e59')
  })

  it('routes sidebar-direction events to sidebarEvents, not the node ring buffer', () => {
    useDebugStore.getState().connect('flow-1')
    lastSocket().onMessage(nodeEvent({ direction: 'sidebar', label: 'my-debug' }))
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(1)
    expect(useDebugStore.getState().nodeEvents['n1']).toBeUndefined()
  })

  it('dedupes by event id (e.g. an old closing socket and a new one briefly overlapping)', () => {
    useDebugStore.getState().connect('flow-1')
    const socket = lastSocket()
    socket.onMessage(nodeEvent({ id: 'dup-1', direction: 'out' }))
    socket.onMessage(nodeEvent({ id: 'dup-1', direction: 'out' })) // redelivered, e.g. by a second overlapping socket
    expect(useDebugStore.getState().nodeEvents['n1']).toHaveLength(1)

    socket.onMessage(nodeEvent({ id: 'dup-2', direction: 'sidebar' }))
    socket.onMessage(nodeEvent({ id: 'dup-2', direction: 'sidebar' }))
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(1)
  })

  it('sidebarPaused blocks new sidebar events until resumed', () => {
    useDebugStore.getState().connect('flow-1')
    useDebugStore.getState().toggleSidebarPaused()
    expect(useDebugStore.getState().sidebarPaused).toBe(true)

    lastSocket().onMessage(nodeEvent({ direction: 'sidebar' }))
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(0)

    useDebugStore.getState().toggleSidebarPaused()
    lastSocket().onMessage(nodeEvent({ direction: 'sidebar', id: 'e2' }))
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(1)
  })

  it('clearSidebar empties the sidebar list', () => {
    useDebugStore.getState().connect('flow-1')
    lastSocket().onMessage(nodeEvent({ direction: 'sidebar' }))
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(1)
    useDebugStore.getState().clearSidebar()
    expect(useDebugStore.getState().sidebarEvents).toHaveLength(0)
  })

  it('marks a source wire as pulsing on an "out" event and clears it after the pulse window (DBG-120)', () => {
    vi.useFakeTimers()
    useDebugStore.getState().connect('flow-1')
    lastSocket().onMessage(nodeEvent({ nodeId: 'n1', port: 'out', direction: 'out' }))

    const key = sourceWireKey('n1', 'out')
    expect(useDebugStore.getState().pulsingSourceWires[key]).toBe(1)

    vi.advanceTimersByTime(399)
    expect(useDebugStore.getState().pulsingSourceWires[key]).toBe(1)
    vi.advanceTimersByTime(50)
    expect(useDebugStore.getState().pulsingSourceWires[key]).toBeUndefined()
  })

  it('an "in" event does not trigger a pulse', () => {
    useDebugStore.getState().connect('flow-1')
    lastSocket().onMessage(nodeEvent({ nodeId: 'n1', port: 'in', direction: 'in' }))
    expect(useDebugStore.getState().pulsingSourceWires).toEqual({})
  })

  it('stores wireMetrics keyed by the full (from, to) wire tuple (DBG-120 counters)', () => {
    useDebugStore.getState().connect('flow-1')
    lastSocket().onMessage({
      type: 'wireMetrics',
      metrics: { flowId: 'flow-1', fromNode: 'n1', fromPort: 'out', toNode: 'n2', toPort: 'in', delivered: 42, dropped: 3 },
    })
    const key = wireKey('n1', 'out', 'n2', 'in')
    expect(useDebugStore.getState().wireMetrics[key]).toEqual({
      flowId: 'flow-1',
      fromNode: 'n1',
      fromPort: 'out',
      toNode: 'n2',
      toPort: 'in',
      delivered: 42,
      dropped: 3,
    })
  })

  it('disconnect closes the socket and clears flowId/connected', () => {
    useDebugStore.getState().connect('flow-1')
    useDebugStore.getState().disconnect()
    expect(lastSocket().closed).toBe(true)
    expect(useDebugStore.getState().flowId).toBeNull()
    expect(useDebugStore.getState().connected).toBe(false)
  })
})
