import { describe, expect, it } from 'vitest'
import { canvasToContent, contentToCanvas, newCanvasNode } from './flowConversion'
import type { FlowFileContent, NodeType } from '../api/types'

const baseContent: FlowFileContent = {
  formatVersion: 1,
  kind: 'flow',
  id: 'flow_test',
  name: 'test',
  mode: 'streaming',
  graph: {
    nodes: [
      { id: 'n1', type: 'inject', typeVersion: 1, name: 'Inject', config: { payload: 1 } },
      { id: 'n2', type: 'debug-log', typeVersion: 1, config: {} },
    ],
    wires: [{ id: 'w1', from: { node: 'n1', port: 'out' }, to: { node: 'n2', port: 'in' } }],
  },
  layout: { nodes: { n1: { x: 10, y: 20 }, n2: { x: 200, y: 20 } } },
}

describe('flowConversion', () => {
  it('round-trips content -> canvas -> content preserving nodes, wires, and positions', () => {
    const { nodes, edges } = contentToCanvas(baseContent)
    expect(nodes).toHaveLength(2)
    expect(edges).toHaveLength(1)
    expect(nodes[0].position).toEqual({ x: 10, y: 20 })

    const roundTripped = canvasToContent(baseContent, nodes, edges)
    expect(roundTripped.graph.nodes).toEqual(baseContent.graph.nodes)
    expect(roundTripped.graph.wires).toEqual(baseContent.graph.wires)
    expect(roundTripped.layout?.nodes).toEqual(baseContent.layout?.nodes)
    // Fields the canvas doesn't own must be untouched.
    expect(roundTripped.id).toBe(baseContent.id)
    expect(roundTripped.mode).toBe(baseContent.mode)
  })

  it('round-trips a node.connection reference without dropping it', () => {
    const content: FlowFileContent = {
      ...baseContent,
      graph: {
        ...baseContent.graph,
        nodes: [
          { id: 'n1', type: 'secsgem-in', typeVersion: 1, connection: 'conn_equip1', config: {} },
          { id: 'n2', type: 'debug-log', typeVersion: 1, config: {} },
        ],
      },
    }
    const { nodes, edges } = contentToCanvas(content)
    expect(nodes[0].data.connection).toBe('conn_equip1')
    expect(nodes[1].data.connection).toBeUndefined()

    const roundTripped = canvasToContent(content, nodes, edges)
    expect(roundTripped.graph.nodes[0].connection).toBe('conn_equip1')
    expect(roundTripped.graph.nodes[1].connection).toBeUndefined()
  })

  it('assigns a default grid position when a node has no stored layout', () => {
    const content: FlowFileContent = { ...baseContent, layout: undefined }
    const { nodes } = contentToCanvas(content)
    expect(nodes[0].position).toEqual({ x: 80, y: 80 })
    expect(nodes[1].position).toEqual({ x: 280, y: 80 })
  })

  it('newCanvasNode creates an empty-config node of the given type at the given position', () => {
    const nodeType: NodeType = {
      type: 'set',
      displayName: 'Set',
      category: 'processor',
      description: '',
      kind: 'processor',
      inputs: ['in'],
      outputs: ['out'],
      configSchema: {},
    }
    const node = newCanvasNode(nodeType, { x: 5, y: 6 })
    expect(node.data.nodeType).toBe('set')
    expect(node.data.config).toEqual({})
    expect(node.position).toEqual({ x: 5, y: 6 })
    expect(node.id).toMatch(/^n_/)
  })

  it('moving a node updates only its own layout position on round-trip', () => {
    const { nodes, edges } = contentToCanvas(baseContent)
    nodes[0] = { ...nodes[0], position: { x: 999, y: 999 } }
    const updated = canvasToContent(baseContent, nodes, edges)
    expect(updated.layout?.nodes?.n1).toEqual({ x: 999, y: 999 })
    expect(updated.layout?.nodes?.n2).toEqual({ x: 200, y: 20 })
  })
})
