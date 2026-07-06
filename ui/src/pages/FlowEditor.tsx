import { useCallback, useEffect, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { ReactFlowProvider } from '@xyflow/react'
import * as api from '../api/resources'
import { ApiError } from '../api/client'
import type { Flow, NodeType } from '../api/types'
import { useEditorStore } from '../store/editor'
import { Palette } from '../components/Palette'
import { FlowCanvas } from '../components/FlowCanvas'
import { ConfigPanel } from '../components/ConfigPanel'
import { canvasToContent, contentToCanvas } from '../utils/flowConversion'
import { useI18n } from '../i18n'

export default function FlowEditor() {
  const { t } = useI18n()
  const { flowId, projectId } = useParams<{ flowId: string; projectId: string }>()
  const [flow, setFlow] = useState<Flow | null>(null)
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([])
  const [saving, setSaving] = useState(false)
  const [deployState, setDeployState] = useState<'idle' | 'deploying' | 'success' | 'error'>('idle')
  const [deployMessage, setDeployMessage] = useState('')
  const [deployComment, setDeployComment] = useState('')

  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)
  const flowDisabled = useEditorStore((s) => s.flowDisabled)
  const selectedNodeId = useEditorStore((s) => s.selectedNodeId)
  const load = useEditorStore((s) => s.load)
  const select = useEditorStore((s) => s.select)
  const updateNodeData = useEditorStore((s) => s.updateNodeData)
  const deleteSelection = useEditorStore((s) => s.deleteSelection)
  const duplicateSelection = useEditorStore((s) => s.duplicateSelection)
  const copySelection = useEditorStore((s) => s.copySelection)
  const paste = useEditorStore((s) => s.paste)
  const toggleFlowDisabled = useEditorStore((s) => s.toggleFlowDisabled)
  const undo = useEditorStore((s) => s.undo)
  const redo = useEditorStore((s) => s.redo)
  const canUndo = useEditorStore((s) => s.past.length > 0)
  const canRedo = useEditorStore((s) => s.future.length > 0)

  useEffect(() => {
    if (!flowId) return
    void api.getFlow(flowId).then((f) => {
      setFlow(f)
      const { nodes, edges } = contentToCanvas(f.content)
      load(nodes, edges, f.content.disabled ?? false)
    })
    void api.listNodeTypes().then(setNodeTypes)
  }, [flowId, load])

  const selectedNode = useMemo(() => nodes.find((n) => n.id === selectedNodeId), [nodes, selectedNodeId])
  const selectedNodeType = useMemo(
    () => nodeTypes.find((n) => n.type === selectedNode?.data.nodeType),
    [nodeTypes, selectedNode],
  )

  const buildContent = useCallback(() => {
    if (!flow) return null
    return canvasToContent({ ...flow.content, disabled: flowDisabled }, nodes, edges)
  }, [flow, nodes, edges, flowDisabled])

  async function onSave() {
    if (!flow) return
    setSaving(true)
    try {
      const content = buildContent()
      if (!content) return
      const updated = await api.updateFlow(flow.id, { content })
      setFlow(updated)
    } finally {
      setSaving(false)
    }
  }

  async function onDeploy() {
    if (!flow) return
    setDeployState('deploying')
    try {
      const content = buildContent()
      if (!content) return
      await api.updateFlow(flow.id, { content })
      const version = await api.deployFlow(flow.id, deployComment)
      setDeployState('success')
      setDeployMessage(t('editor.deploy.success', { version: version.version }))
      const refreshed = await api.getFlow(flow.id)
      setFlow(refreshed)
    } catch (err) {
      setDeployState('error')
      if (err instanceof ApiError && err.status === 409) {
        setDeployMessage(t('editor.deploy.noRuntime'))
      } else if (err instanceof ApiError) {
        setDeployMessage(err.message)
      } else {
        setDeployMessage(t('editor.deploy.error'))
      }
    }
  }

  // UI-160: keyboard-first operation.
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const target = e.target as HTMLElement
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName)) return
      const meta = e.metaKey || e.ctrlKey
      if (meta && e.key.toLowerCase() === 'z' && e.shiftKey) {
        e.preventDefault()
        redo()
      } else if (meta && e.key.toLowerCase() === 'z') {
        e.preventDefault()
        undo()
      } else if (meta && e.key.toLowerCase() === 'c') {
        copySelection()
      } else if (meta && e.key.toLowerCase() === 'v') {
        paste()
      } else if (meta && e.key.toLowerCase() === 'd') {
        e.preventDefault()
        duplicateSelection()
      } else if (e.key === 'Delete' || e.key === 'Backspace') {
        if (selectedNodeId) deleteSelection()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [undo, redo, copySelection, paste, duplicateSelection, deleteSelection, selectedNodeId])

  if (!flow) {
    return <div className="p-6 text-sm text-(--color-text-muted)">{t('common.loading')}</div>
  }

  return (
    <div className="flex h-[calc(100vh-41px)] flex-col">
      <div className="flex items-center justify-between border-b border-(--color-border) px-3 py-1.5 text-sm">
        <div className="flex items-center gap-2">
          <Link to={`/projects/${projectId}`} className="text-(--color-accent)">
            ←
          </Link>
          <span className="font-medium">{flow.name}</span>
          <span className="text-xs text-(--color-text-muted)">
            {flow.deployedVersion != null ? t('flows.deployedVersion', { version: flow.deployedVersion }) : t('flows.notDeployed')}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <button onClick={undo} disabled={!canUndo} className="rounded border border-(--color-border) px-2 py-1 disabled:opacity-40">
            {t('editor.undo')}
          </button>
          <button onClick={redo} disabled={!canRedo} className="rounded border border-(--color-border) px-2 py-1 disabled:opacity-40">
            {t('editor.redo')}
          </button>
          <button onClick={toggleFlowDisabled} className="rounded border border-(--color-border) px-2 py-1">
            {flowDisabled ? t('editor.flow.enable') : t('editor.flow.disable')}
          </button>
          <button onClick={() => void onSave()} disabled={saving} className="rounded border border-(--color-border) px-2 py-1">
            {t('editor.save')}
          </button>
          <input
            className="w-40 rounded border border-(--color-border) bg-transparent px-2 py-1"
            placeholder={t('editor.deploy.comment')}
            value={deployComment}
            onChange={(e) => setDeployComment(e.target.value)}
          />
          <button
            onClick={() => void onDeploy()}
            disabled={deployState === 'deploying'}
            className="rounded bg-(--color-accent) px-3 py-1 font-medium text-white disabled:opacity-50"
          >
            {deployState === 'deploying' ? t('editor.deploy.deploying') : t('editor.deploy')}
          </button>
        </div>
      </div>

      {deployState === 'success' && (
        <div className="border-b border-(--color-border) bg-green-50 px-3 py-1.5 text-sm text-green-800">{deployMessage}</div>
      )}
      {deployState === 'error' && (
        <div role="alert" className="border-b border-(--color-border) bg-red-50 px-3 py-1.5 text-sm text-red-800">
          {deployMessage}
        </div>
      )}

      <div className="flex flex-1 overflow-hidden">
        <Palette nodeTypes={nodeTypes} />
        <ReactFlowProvider>
          <FlowCanvas nodeTypes={nodeTypes} />
        </ReactFlowProvider>
        {selectedNode && (
          <ConfigPanel
            node={{ name: selectedNode.data.name, config: selectedNode.data.config }}
            nodeType={selectedNodeType}
            onChange={(patch) => updateNodeData(selectedNode.id, patch)}
            onClose={() => select(null)}
          />
        )}
      </div>
    </div>
  )
}
