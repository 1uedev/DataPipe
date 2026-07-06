import { useCallback, useMemo, useRef, useState } from 'react'
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useReactFlow,
  type NodeMouseHandler,
  type NodeTypes,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import type { NodeType } from '../api/types'
import { useEditorStore } from '../store/editor'
import { usePalettePrefsStore } from '../store/palettePrefs'
import { useDebugStore, sourceWireKey, wireKey } from '../store/debug'
import { newCanvasNode, type CanvasNode } from '../utils/flowConversion'
import { makeFlowNodeView } from './FlowNodeView'
import { useI18n } from '../i18n'

interface FlowCanvasProps {
  nodeTypes: NodeType[]
}

// UI-100: infinite canvas, pan/zoom, grid snap + minimap toggles.
export function FlowCanvas({ nodeTypes }: FlowCanvasProps) {
  const { t } = useI18n()
  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)
  const onNodesChange = useEditorStore((s) => s.onNodesChange)
  const onEdgesChange = useEditorStore((s) => s.onEdgesChange)
  const onConnect = useEditorStore((s) => s.onConnect)
  const reconnectEdge = useEditorStore((s) => s.reconnectEdge)
  const select = useEditorStore((s) => s.select)
  const addNode = useEditorStore((s) => s.addNode)
  const commit = useEditorStore((s) => s.commit)
  const recordUsed = usePalettePrefsStore((s) => s.recordUsed)
  const pulsingSourceWires = useDebugStore((s) => s.pulsingSourceWires)
  const wireMetrics = useDebugStore((s) => s.wireMetrics)

  const { screenToFlowPosition, fitView } = useReactFlow()
  const wrapperRef = useRef<HTMLDivElement>(null)
  const [gridSnap, setGridSnap] = useState(true)
  const [showMinimap, setShowMinimap] = useState(true)

  const nodeTypesByName = useMemo(() => new Map(nodeTypes.map((n) => [n.type, n])), [nodeTypes])
  const rfNodeTypes: NodeTypes = useMemo(() => ({ flowNode: makeFlowNodeView(nodeTypesByName) }), [nodeTypesByName])

  // DBG-120: wires visibly pulse as datagrams pass, with live counters —
  // pulses come from per-node "out" events (source side only, so a fan-out
  // to several wires pulses all of them together); counters come from the
  // periodic per-wire metrics snapshot (accurate even while pulses are
  // themselves rate-limited/sampled, DBG-170).
  const decoratedEdges = useMemo(
    () =>
      edges.map((e) => {
        const sourcePort = e.sourceHandle ?? ''
        const targetPort = e.targetHandle ?? ''
        const pulsing = sourceWireKey(e.source, sourcePort) in pulsingSourceWires
        const metrics = wireMetrics[wireKey(e.source, sourcePort, e.target, targetPort)]
        const label = metrics ? `${metrics.delivered}${metrics.dropped ? ` (-${metrics.dropped})` : ''}` : undefined
        return { ...e, animated: pulsing || e.animated, className: pulsing ? 'debug-pulse' : undefined, label }
      }),
    [edges, pulsingSourceWires, wireMetrics],
  )

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const typeName = e.dataTransfer.getData('application/datapipe-node-type')
      const nodeType = nodeTypesByName.get(typeName)
      if (!nodeType) return
      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
      addNode(newCanvasNode(nodeType, position))
      recordUsed(typeName)
    },
    [nodeTypesByName, screenToFlowPosition, addNode, recordUsed],
  )

  const onNodeClick: NodeMouseHandler<CanvasNode> = useCallback((_, node) => select(node.id), [select])
  const onPaneClick = useCallback(() => select(null), [select])
  const onNodeDragStop = useCallback(() => commit(), [commit])

  return (
    <div ref={wrapperRef} className="relative h-full flex-1">
      <ReactFlow
        nodes={nodes}
        edges={decoratedEdges}
        nodeTypes={rfNodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onReconnect={(oldEdge, connection) => reconnectEdge(oldEdge.id, connection)}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        onNodeDragStart={commit}
        onNodeDragStop={onNodeDragStop}
        onDragOver={onDragOver}
        onDrop={onDrop}
        snapToGrid={gridSnap}
        snapGrid={[16, 16]}
        minZoom={0.1}
        maxZoom={4}
        fitView
      >
        <Background gap={16} />
        <Controls showInteractive={false} />
        {showMinimap && <MiniMap pannable zoomable />}
      </ReactFlow>
      <div className="absolute top-2 right-2 flex gap-1 text-xs">
        <button
          onClick={() => setGridSnap((v) => !v)}
          title={t('editor.canvas.toggleGrid')}
          className={`rounded border border-(--color-border) bg-(--color-bg) px-2 py-1 ${gridSnap ? 'text-(--color-accent)' : ''}`}
        >
          #
        </button>
        <button
          onClick={() => setShowMinimap((v) => !v)}
          title={t('editor.canvas.toggleMinimap')}
          className={`rounded border border-(--color-border) bg-(--color-bg) px-2 py-1 ${showMinimap ? 'text-(--color-accent)' : ''}`}
        >
          ▤
        </button>
        <button
          onClick={() => fitView()}
          title={t('editor.canvas.fitView')}
          className="rounded border border-(--color-border) bg-(--color-bg) px-2 py-1"
        >
          ⤢
        </button>
      </div>
    </div>
  )
}
