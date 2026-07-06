import { beforeEach, describe, expect, it } from 'vitest'
import { useEditorStore } from './editor'
import type { CanvasEdge, CanvasNode } from '../utils/flowConversion'

function node(id: string, x = 0): CanvasNode {
  return { id, type: 'flowNode', position: { x, y: 0 }, data: { nodeType: 'set', config: {}, disabled: false, typeVersion: 1 } }
}
function edge(id: string, source: string, target: string): CanvasEdge {
  return { id, source, sourceHandle: 'out', target, targetHandle: 'in' }
}

describe('editor store', () => {
  beforeEach(() => {
    useEditorStore.setState({ nodes: [], edges: [], selectedNodeId: null, flowDisabled: false, past: [], future: [], clipboard: null })
  })

  it('undo/redo restores prior node/edge state', () => {
    const s = useEditorStore.getState()
    s.load([node('n1')], [], false)
    s.addNode(node('n2', 100))
    expect(useEditorStore.getState().nodes).toHaveLength(2)

    useEditorStore.getState().undo()
    expect(useEditorStore.getState().nodes).toHaveLength(1)

    useEditorStore.getState().redo()
    expect(useEditorStore.getState().nodes).toHaveLength(2)
  })

  it('caps history at 100 entries', () => {
    const s = useEditorStore.getState()
    s.load([node('n0')], [], false)
    for (let i = 0; i < 150; i++) {
      useEditorStore.getState().addNode(node(`n${i + 1}`))
    }
    expect(useEditorStore.getState().past.length).toBeLessThanOrEqual(100)
  })

  it('deleteSelection heals the wire through a single-in/single-out node', () => {
    const s = useEditorStore.getState()
    s.load([node('n1'), node('n2', 100), node('n3', 200)], [edge('w1', 'n1', 'n2'), edge('w2', 'n2', 'n3')], false)
    s.select('n2')
    useEditorStore.getState().deleteSelection()

    const state = useEditorStore.getState()
    expect(state.nodes.map((n) => n.id)).toEqual(['n1', 'n3'])
    expect(state.edges).toHaveLength(1)
    expect(state.edges[0]).toMatchObject({ source: 'n1', target: 'n3' })
  })

  it('deleteSelection does not heal when the node has multiple in/out wires', () => {
    const s = useEditorStore.getState()
    s.load(
      [node('n1'), node('n2', 100), node('n3', 200), node('n4', 300)],
      [edge('w1', 'n1', 'n2'), edge('w2', 'n3', 'n2'), edge('w3', 'n2', 'n4')],
      false,
    )
    s.select('n2')
    useEditorStore.getState().deleteSelection()
    expect(useEditorStore.getState().edges).toHaveLength(0)
  })

  it('duplicateSelection creates a new node with a fresh id, offset position', () => {
    const s = useEditorStore.getState()
    s.load([node('n1')], [], false)
    s.select('n1')
    useEditorStore.getState().duplicateSelection()

    const state = useEditorStore.getState()
    expect(state.nodes).toHaveLength(2)
    expect(state.nodes[1].id).not.toBe('n1')
    expect(state.nodes[1].position.x).toBe(40)
  })

  it('copy then paste duplicates the copied node into the canvas', () => {
    const s = useEditorStore.getState()
    s.load([node('n1')], [], false)
    s.select('n1')
    useEditorStore.getState().copySelection()
    useEditorStore.getState().paste()

    expect(useEditorStore.getState().nodes).toHaveLength(2)
  })

  it('reconnectEdge moves an existing wire to a new target (detach/reattach)', () => {
    const s = useEditorStore.getState()
    s.load([node('n1'), node('n2', 100), node('n3', 200)], [edge('w1', 'n1', 'n2')], false)

    useEditorStore.getState().reconnectEdge('w1', { source: 'n1', sourceHandle: 'out', target: 'n3', targetHandle: 'in' })

    const state = useEditorStore.getState()
    expect(state.edges).toHaveLength(1)
    expect(state.edges[0]).toMatchObject({ source: 'n1', target: 'n3' })

    useEditorStore.getState().undo()
    expect(useEditorStore.getState().edges[0]).toMatchObject({ source: 'n1', target: 'n2' })
  })

  it('toggleFlowDisabled flips flowDisabled and is undoable', () => {
    const s = useEditorStore.getState()
    s.load([], [], false)
    useEditorStore.getState().toggleFlowDisabled()
    expect(useEditorStore.getState().flowDisabled).toBe(true)
    useEditorStore.getState().undo()
    expect(useEditorStore.getState().flowDisabled).toBe(false)
  })
})
