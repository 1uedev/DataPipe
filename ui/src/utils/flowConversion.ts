import type { Edge, Node } from '@xyflow/react'
import type { FlowFileContent, FlowNode as ApiFlowNode, NodeType } from '../api/types'
import { newId } from './id'

export interface CanvasNodeData extends Record<string, unknown> {
  nodeType: string
  name?: string
  config: Record<string, unknown>
  disabled: boolean
  connection?: string
  errorPolicy?: ApiFlowNode['errorPolicy']
  overflow?: string
  typeVersion: number
}

export type CanvasNode = Node<CanvasNodeData, 'flowNode'>
export type CanvasEdge = Edge

// Converts a stored FlowFileContent into React Flow's node/edge shape.
export function contentToCanvas(content: FlowFileContent): { nodes: CanvasNode[]; edges: CanvasEdge[] } {
  const positions = content.layout?.nodes ?? {}
  const nodes: CanvasNode[] = content.graph.nodes.map((n, i) => ({
    id: n.id,
    type: 'flowNode',
    position: positions[n.id] ?? { x: 80 + (i % 5) * 200, y: 80 + Math.floor(i / 5) * 140 },
    data: {
      nodeType: n.type,
      name: n.name,
      config: n.config ?? {},
      disabled: n.disabled ?? false,
      connection: n.connection,
      errorPolicy: n.errorPolicy,
      overflow: n.overflow,
      typeVersion: n.typeVersion,
    },
  }))

  const edges: CanvasEdge[] = content.graph.wires.map((w) => ({
    id: w.id,
    source: w.from.node,
    sourceHandle: w.from.port,
    target: w.to.node,
    targetHandle: w.to.port,
  }))

  return { nodes, edges }
}

// Converts the live canvas state back into a FlowFileContent, preserving
// every field the canvas doesn't own (formatVersion/kind/id/mode/...).
export function canvasToContent(base: FlowFileContent, nodes: CanvasNode[], edges: CanvasEdge[]): FlowFileContent {
  const graphNodes: ApiFlowNode[] = nodes.map((n) => ({
    id: n.id,
    type: n.data.nodeType,
    typeVersion: n.data.typeVersion,
    name: n.data.name || undefined,
    disabled: n.data.disabled || undefined,
    connection: n.data.connection || undefined,
    config: n.data.config,
    errorPolicy: n.data.errorPolicy,
    overflow: n.data.overflow,
  }))

  const wires = edges.map((e) => ({
    id: e.id,
    from: { node: e.source, port: e.sourceHandle ?? 'out' },
    to: { node: e.target, port: e.targetHandle ?? 'in' },
  }))

  const layoutNodes: Record<string, { x: number; y: number }> = {}
  for (const n of nodes) layoutNodes[n.id] = { x: n.position.x, y: n.position.y }

  return {
    ...base,
    graph: { nodes: graphNodes, wires },
    layout: { ...base.layout, nodes: layoutNodes },
  }
}

// Builds a brand-new canvas node for a node type dropped from the palette.
export function newCanvasNode(nodeType: NodeType, position: { x: number; y: number }): CanvasNode {
  return {
    id: newId('n'),
    type: 'flowNode',
    position,
    data: {
      nodeType: nodeType.type,
      config: {},
      disabled: false,
      typeVersion: 1,
    },
  }
}
