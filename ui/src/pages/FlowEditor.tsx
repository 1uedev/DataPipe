import { useCallback, useEffect, useMemo, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { ReactFlowProvider } from '@xyflow/react'
import * as api from '../api/resources'
import { ApiError } from '../api/client'
import type { EnvironmentProfile, Flow, NodeType, RuntimeGroup } from '../api/types'
import { useEditorStore } from '../store/editor'
import { Palette } from '../components/Palette'
import { FlowCanvas } from '../components/FlowCanvas'
import { ConfigPanel } from '../components/ConfigPanel'
import { DebugSidebar } from '../components/DebugSidebar'
import { Tutorial } from '../components/Tutorial'
import { canvasToContent, contentToCanvas } from '../utils/flowConversion'
import { downloadJSON } from '../utils/download'
import { useDebugStore } from '../store/debug'
import { useI18n } from '../i18n'

export default function FlowEditor() {
  const { t } = useI18n()
  const { flowId, projectId } = useParams<{ flowId: string; projectId: string }>()
  const [flow, setFlow] = useState<Flow | null>(null)
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([])
  const [runtimeGroups, setRuntimeGroups] = useState<RuntimeGroup[]>([])
  const [runtimeGroup, setRuntimeGroup] = useState('')
  const [envProfiles, setEnvProfiles] = useState<EnvironmentProfile[]>([])
  const [profileId, setProfileId] = useState('')
  const [saving, setSaving] = useState(false)
  const [deployState, setDeployState] = useState<'idle' | 'deploying' | 'success' | 'error'>('idle')
  const [deployMessage, setDeployMessage] = useState('')
  const [deployComment, setDeployComment] = useState('')
  const [showDebugSidebar, setShowDebugSidebar] = useState(false)
  const [showTutorial, setShowTutorial] = useState(false)

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

  const connectDebug = useDebugStore((s) => s.connect)
  const disconnectDebug = useDebugStore((s) => s.disconnect)

  useEffect(() => {
    if (!flowId) return
    void api.getFlow(flowId).then((f) => {
      setFlow(f)
      setRuntimeGroup(f.content.runtimeAssignment?.group ?? '')
      setProfileId(f.activeProfileId ?? '')
      const { nodes, edges } = contentToCanvas(f.content)
      load(nodes, edges, f.content.disabled ?? false)
      setShowTutorial(f.content.graph.nodes.length === 0)
      void api.listEnvProfiles(f.projectId).then(setEnvProfiles)
    })
    void api.listNodeTypes().then(setNodeTypes)
    void api.listRuntimeGroups().then(setRuntimeGroups)
  }, [flowId, load])

  // DBG-100/110/120/170: one live-debugging subscription per open flow.
  useEffect(() => {
    if (!flowId) return
    connectDebug(flowId)
    return () => disconnectDebug()
  }, [flowId, connectDebug, disconnectDebug])

  const selectedNode = useMemo(() => nodes.find((n) => n.id === selectedNodeId), [nodes, selectedNodeId])
  const selectedNodeType = useMemo(
    () => nodeTypes.find((n) => n.type === selectedNode?.data.nodeType),
    [nodeTypes, selectedNode],
  )

  const buildContent = useCallback(() => {
    if (!flow) return null
    return canvasToContent(
      { ...flow.content, disabled: flowDisabled, runtimeAssignment: runtimeGroup ? { group: runtimeGroup } : null },
      nodes,
      edges,
    )
  }, [flow, nodes, edges, flowDisabled, runtimeGroup])

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

  async function onExport() {
    if (!flow) return
    const bundle = await api.exportFlow(flow.id)
    downloadJSON(`${flow.name}.flow.json`, bundle)
  }

  async function onLogLevelChange(level: Flow['logLevel']) {
    if (!flow) return
    const updated = await api.setFlowLogLevel(flow.id, level)
    setFlow(updated)
  }

  async function onDeploy() {
    if (!flow) return
    setDeployState('deploying')
    try {
      const content = buildContent()
      if (!content) return
      await api.updateFlow(flow.id, { content })
      const version = await api.deployFlow(flow.id, deployComment, profileId)
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
          <select
            aria-label={t('editor.deployTarget')}
            title={t('editor.deployTarget')}
            value={runtimeGroup}
            onChange={(e) => setRuntimeGroup(e.target.value)}
            className="rounded border border-(--color-border) bg-transparent px-1.5 py-1 text-xs"
          >
            <option value="">{t('editor.deployTarget.default')}</option>
            {runtimeGroups.map((g) => (
              <option key={g.name} value={g.name}>
                {g.name}
              </option>
            ))}
          </select>
          <select
            aria-label={t('editor.logLevel')}
            title={t('editor.logLevel')}
            value={flow.logLevel}
            onChange={(e) => void onLogLevelChange(e.target.value as Flow['logLevel'])}
            className="rounded border border-(--color-border) bg-transparent px-1.5 py-1 text-xs"
          >
            <option value="debug">{t('editor.logLevel.debug')}</option>
            <option value="info">{t('editor.logLevel.info')}</option>
            <option value="warn">{t('editor.logLevel.warn')}</option>
            <option value="error">{t('editor.logLevel.error')}</option>
          </select>
          <select
            aria-label={t('editor.envProfile')}
            title={t('editor.envProfile')}
            value={profileId}
            onChange={(e) => setProfileId(e.target.value)}
            className="rounded border border-(--color-border) bg-transparent px-1.5 py-1 text-xs"
          >
            <option value="">{t('editor.envProfile.none')}</option>
            {envProfiles.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-1.5">
          {flow.content.mode === 'triggered' && (
            <Link to={`/projects/${projectId}/flows/${flowId}/executions`} className="rounded border border-(--color-border) px-2 py-1">
              {t('executions.title')}
            </Link>
          )}
          <Link to={`/projects/${projectId}/flows/${flowId}/dead-letters`} className="rounded border border-(--color-border) px-2 py-1">
            {t('deadLetters.title')}
          </Link>
          <button onClick={undo} disabled={!canUndo} className="rounded border border-(--color-border) px-2 py-1 disabled:opacity-40">
            {t('editor.undo')}
          </button>
          <button onClick={redo} disabled={!canRedo} className="rounded border border-(--color-border) px-2 py-1 disabled:opacity-40">
            {t('editor.redo')}
          </button>
          <button onClick={toggleFlowDisabled} className="rounded border border-(--color-border) px-2 py-1">
            {flowDisabled ? t('editor.flow.enable') : t('editor.flow.disable')}
          </button>
          <button
            onClick={() => setShowDebugSidebar((v) => !v)}
            title={t('debugSidebar.toggle')}
            className={`rounded border border-(--color-border) px-2 py-1 ${showDebugSidebar ? 'text-(--color-accent)' : ''}`}
          >
            {t('debugSidebar.title')}
          </button>
          <button
            onClick={() => setShowTutorial((v) => !v)}
            title={t('tutorial.title')}
            className={`rounded border border-(--color-border) px-2 py-1 ${showTutorial ? 'text-(--color-accent)' : ''}`}
          >
            {t('tutorial.title')}
          </button>
          <button onClick={() => void onSave()} disabled={saving} className="rounded border border-(--color-border) px-2 py-1">
            {t('editor.save')}
          </button>
          <button onClick={() => void onExport()} className="rounded border border-(--color-border) px-2 py-1">
            {t('editor.export')}
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
        <div className="relative flex flex-1 flex-col overflow-hidden">
          <ReactFlowProvider>
            <FlowCanvas nodeTypes={nodeTypes} />
          </ReactFlowProvider>
          {showTutorial && <Tutorial deployed={flow.deployedVersion != null} onClose={() => setShowTutorial(false)} />}
          {showDebugSidebar && <DebugSidebar onClose={() => setShowDebugSidebar(false)} />}
        </div>
        {selectedNode && (
          <ConfigPanel
            node={{ name: selectedNode.data.name, config: selectedNode.data.config, connection: selectedNode.data.connection }}
            nodeType={selectedNodeType}
            flowId={flow.id}
            nodeId={selectedNode.id}
            projectId={flow.projectId}
            onChange={(patch) => updateNodeData(selectedNode.id, patch)}
            onClose={() => select(null)}
          />
        )}
      </div>
    </div>
  )
}
