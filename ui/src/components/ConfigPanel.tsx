import { useEffect, useState } from 'react'
import * as api from '../api/resources'
import type { Connection, NodeType } from '../api/types'
import { SchemaForm } from './SchemaForm'
import { Inspector } from './Inspector'
import { SecsgemReportBuilder } from './SecsgemReportBuilder'
import { useI18n } from '../i18n'

// The subset of a node's editable fields this panel actually produces —
// kept narrow (rather than the full API FlowNode or full CanvasNodeData)
// so callers can pass either without a cast.
export interface ConfigPanelNode extends Record<string, unknown> {
  name?: string
  config?: Record<string, unknown>
  connection?: string
}

interface ConfigPanelProps {
  node: ConfigPanelNode
  nodeType: NodeType | undefined
  flowId: string
  nodeId: string
  projectId: string
  onChange: (patch: ConfigPanelNode) => void
  onClose: () => void
}

// UI-170: "selecting a node opens a right-hand panel (not modal) with
// configuration, description, and node documentation tab; required-field
// validation with inline errors before deploy." The "Inspect" tab adds
// Increment 5's live data / run-once / pinning (DBG-100/130).
export function ConfigPanel({ node, nodeType, flowId, nodeId, projectId, onChange, onClose }: ConfigPanelProps) {
  const { t } = useI18n()
  const [tab, setTab] = useState<'config' | 'description' | 'inspect'>('config')
  const [connections, setConnections] = useState<Connection[]>([])

  useEffect(() => {
    let cancelled = false
    void api.listConnections(projectId).then((all) => {
      if (!cancelled) setConnections(all)
    })
    return () => {
      cancelled = true
    }
  }, [projectId])

  return (
    <aside className="flex h-full w-80 flex-col border-l border-(--color-border) bg-(--color-bg)">
      <div className="flex items-center justify-between border-b border-(--color-border) px-3 py-2">
        <h2 className="text-sm font-semibold">{t('config.title')}</h2>
        <button onClick={onClose} aria-label={t('common.close')} className="text-sm">
          ✕
        </button>
      </div>

      <label className="block px-3 pt-3 text-sm">
        {t('config.name')}
        <input
          className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
          value={node.name ?? ''}
          onChange={(e) => onChange({ name: e.target.value })}
          placeholder={nodeType?.displayName ?? nodeType?.type}
        />
      </label>

      {connections.length > 0 && (
        <label className="block px-3 pt-2 text-sm">
          {t('config.connection')}
          <select
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={node.connection ?? ''}
            onChange={(e) => onChange({ connection: e.target.value || undefined })}
          >
            <option value="">{t('config.connection.none')}</option>
            {connections.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} ({c.type})
              </option>
            ))}
          </select>
        </label>
      )}

      <div className="mt-3 flex border-b border-(--color-border) text-sm">
        <button
          onClick={() => setTab('config')}
          className={`flex-1 border-b-2 px-3 py-2 ${tab === 'config' ? 'border-(--color-accent) font-medium' : 'border-transparent text-(--color-text-muted)'}`}
        >
          {t('config.tab.config')}
        </button>
        <button
          onClick={() => setTab('description')}
          className={`flex-1 border-b-2 px-3 py-2 ${tab === 'description' ? 'border-(--color-accent) font-medium' : 'border-transparent text-(--color-text-muted)'}`}
        >
          {t('config.tab.description')}
        </button>
        <button
          onClick={() => setTab('inspect')}
          className={`flex-1 border-b-2 px-3 py-2 ${tab === 'inspect' ? 'border-(--color-accent) font-medium' : 'border-transparent text-(--color-text-muted)'}`}
        >
          {t('config.tab.inspect')}
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-3">
        {tab === 'config' &&
          (nodeType ? (
            <>
              <SchemaForm
                schema={nodeType.configSchema}
                value={node.config ?? {}}
                onChange={(v) => onChange({ config: v as Record<string, unknown> })}
              />
              {nodeType.type === 'secsgem-in' && <SecsgemReportBuilder connectionId={node.connection} />}
            </>
          ) : null)}
        {tab === 'description' && <p className="text-sm text-(--color-text-muted)">{nodeType?.description}</p>}
        {tab === 'inspect' && <Inspector flowId={flowId} nodeId={nodeId} nodeKind={nodeType?.kind} />}
      </div>
    </aside>
  )
}
