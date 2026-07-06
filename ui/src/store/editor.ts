import { create } from 'zustand'
import {
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  reconnectEdge as applyReconnectEdge,
  type NodeChange,
  type EdgeChange,
  type Connection,
} from '@xyflow/react'
import type { CanvasEdge, CanvasNode } from '../utils/flowConversion'
import { newId } from '../utils/id'

// UI-160: undo/redo (min 100 steps), copy/paste (portable JSON), duplicate,
// delete with wire healing.
const MAX_HISTORY = 100

interface Snapshot {
  nodes: CanvasNode[]
  edges: CanvasEdge[]
  flowDisabled: boolean
}

interface ClipboardData {
  nodes: CanvasNode[]
  edges: CanvasEdge[]
}

interface EditorState {
  nodes: CanvasNode[]
  edges: CanvasEdge[]
  selectedNodeId: string | null
  flowDisabled: boolean
  past: Snapshot[]
  future: Snapshot[]
  clipboard: ClipboardData | null

  load: (nodes: CanvasNode[], edges: CanvasEdge[], flowDisabled: boolean) => void
  onNodesChange: (changes: NodeChange<CanvasNode>[]) => void
  onEdgesChange: (changes: EdgeChange<CanvasEdge>[]) => void
  onConnect: (connection: Connection) => void
  reconnectEdge: (oldEdgeId: string, connection: Connection) => void
  commit: () => void
  select: (id: string | null) => void
  addNode: (node: CanvasNode) => void
  updateNodeData: (id: string, patch: Partial<CanvasNode['data']>) => void
  deleteSelection: () => void
  duplicateSelection: () => void
  copySelection: () => void
  paste: () => void
  toggleFlowDisabled: () => void
  undo: () => void
  redo: () => void
}

function pushHistory(past: Snapshot[], snapshot: Snapshot): Snapshot[] {
  const next = [...past, snapshot]
  return next.length > MAX_HISTORY ? next.slice(next.length - MAX_HISTORY) : next
}

export const useEditorStore = create<EditorState>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  flowDisabled: false,
  past: [],
  future: [],
  clipboard: null,

  load: (nodes, edges, flowDisabled) => set({ nodes, edges, flowDisabled, past: [], future: [], selectedNodeId: null }),

  onNodesChange: (changes) => {
    // Commit before destructive changes (remove); positional drags commit
    // once on drag-stop (see FlowCanvas), not per intermediate frame.
    if (changes.some((c) => c.type === 'remove')) get().commit()
    set((state) => ({ nodes: applyNodeChanges(changes, state.nodes) }))
  },

  onEdgesChange: (changes) => {
    if (changes.some((c) => c.type === 'remove')) get().commit()
    set((state) => ({ edges: applyEdgeChanges(changes, state.edges) }))
  },

  onConnect: (connection) => {
    get().commit()
    set((state) => ({ edges: addEdge({ ...connection, id: newId('w') }, state.edges) }))
  },

  // UI-130: "detach/reattach wires" — dragging an existing wire's endpoint
  // to a new port rewires it in place instead of requiring delete + redraw.
  reconnectEdge: (oldEdgeId, connection) => {
    get().commit()
    set((state) => {
      const oldEdge = state.edges.find((e) => e.id === oldEdgeId)
      if (!oldEdge) return state
      return { edges: applyReconnectEdge(oldEdge, connection, state.edges) }
    })
  },

  commit: () => {
    set((state) => ({
      past: pushHistory(state.past, { nodes: state.nodes, edges: state.edges, flowDisabled: state.flowDisabled }),
      future: [],
    }))
  },

  select: (id) => set({ selectedNodeId: id }),

  addNode: (node) => {
    get().commit()
    set((state) => ({ nodes: [...state.nodes, node] }))
  },

  updateNodeData: (id, patch) => {
    get().commit()
    set((state) => ({
      nodes: state.nodes.map((n) => (n.id === id ? { ...n, data: { ...n.data, ...patch } } : n)),
    }))
  },

  // "delete with wire healing (reconnect through)": if the deleted node had
  // exactly one incoming and one outgoing wire, reconnect its upstream
  // source directly to its downstream target instead of just orphaning them.
  deleteSelection: () => {
    const { selectedNodeId, nodes, edges } = get()
    if (!selectedNodeId) return
    get().commit()

    const incoming = edges.filter((e) => e.target === selectedNodeId)
    const outgoing = edges.filter((e) => e.source === selectedNodeId)
    const healed: CanvasEdge[] =
      incoming.length === 1 && outgoing.length === 1
        ? [
            {
              id: newId('w'),
              source: incoming[0].source,
              sourceHandle: incoming[0].sourceHandle,
              target: outgoing[0].target,
              targetHandle: outgoing[0].targetHandle,
            },
          ]
        : []

    set({
      nodes: nodes.filter((n) => n.id !== selectedNodeId),
      edges: [...edges.filter((e) => e.source !== selectedNodeId && e.target !== selectedNodeId), ...healed],
      selectedNodeId: null,
    })
  },

  duplicateSelection: () => {
    const { selectedNodeId, nodes } = get()
    const node = nodes.find((n) => n.id === selectedNodeId)
    if (!node) return
    get().commit()
    const copy: CanvasNode = {
      ...node,
      id: newId('n'),
      position: { x: node.position.x + 40, y: node.position.y + 40 },
      selected: false,
    }
    set((state) => ({ nodes: [...state.nodes, copy], selectedNodeId: copy.id }))
  },

  copySelection: () => {
    const { selectedNodeId, nodes } = get()
    const node = nodes.find((n) => n.id === selectedNodeId)
    if (!node) return
    set({ clipboard: { nodes: [node], edges: [] } })
  },

  paste: () => {
    const { clipboard } = get()
    if (!clipboard) return
    get().commit()
    const idMap = new Map<string, string>()
    const pasted = clipboard.nodes.map((n) => {
      const id = newId('n')
      idMap.set(n.id, id)
      return { ...n, id, position: { x: n.position.x + 40, y: n.position.y + 40 }, selected: false }
    })
    set((state) => ({ nodes: [...state.nodes, ...pasted] }))
  },

  toggleFlowDisabled: () => {
    get().commit()
    set((state) => ({ flowDisabled: !state.flowDisabled }))
  },

  undo: () => {
    const { past, nodes, edges, flowDisabled, future } = get()
    if (past.length === 0) return
    const previous = past[past.length - 1]
    set({
      nodes: previous.nodes,
      edges: previous.edges,
      flowDisabled: previous.flowDisabled,
      past: past.slice(0, -1),
      future: [{ nodes, edges, flowDisabled }, ...future],
    })
  },

  redo: () => {
    const { future, nodes, edges, flowDisabled, past } = get()
    if (future.length === 0) return
    const next = future[0]
    set({
      nodes: next.nodes,
      edges: next.edges,
      flowDisabled: next.flowDisabled,
      future: future.slice(1),
      past: pushHistory(past, { nodes, edges, flowDisabled }),
    })
  },
}))
