import { useEffect, useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type { Connection, ConnectionTestResult, Flow, Project } from '../api/types'
import { useI18n } from '../i18n'
import { emptyFlowContent } from '../utils/flowFile'

export default function ProjectDetail() {
  const { t } = useI18n()
  const { projectId } = useParams<{ projectId: string }>()
  const [project, setProject] = useState<Project | null>(null)
  const [flows, setFlows] = useState<Flow[] | null>(null)
  const [name, setName] = useState('')
  const [creating, setCreating] = useState(false)
  const [connections, setConnections] = useState<Connection[] | null>(null)

  useEffect(() => {
    if (!projectId) return
    void api.getProject(projectId).then(setProject)
    void api.listFlows(projectId).then((data) => setFlows(data ?? []))
    void api.listConnections(projectId).then((data) => setConnections(data ?? []))
  }, [projectId])

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    if (!projectId) return
    setCreating(true)
    try {
      const flow = await api.createFlow(projectId, name, emptyFlowContent(name))
      setFlows((prev) => [...(prev ?? []), flow])
      setName('')
    } finally {
      setCreating(false)
    }
  }

  async function onDeleteConnection(id: string) {
    await api.deleteConnection(id)
    setConnections((prev) => (prev ?? []).filter((c) => c.id !== id))
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <Link to="/projects" className="text-sm text-(--color-accent)">
        ← {t('flows.backToProjects')}
      </Link>
      <h1 className="mt-2 mb-4 text-xl font-semibold">{project?.name ?? t('common.loading')}</h1>

      <h2 className="mb-2 text-sm font-semibold text-(--color-text-muted)">{t('flows.title')}</h2>
      {flows === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : flows.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('flows.empty')}</p>
      ) : (
        <ul className="mb-6 divide-y divide-(--color-border) rounded border border-(--color-border)">
          {flows.map((f) => (
            <li key={f.id} className="flex items-center justify-between px-3 py-2">
              <div>
                <div className="text-sm font-medium">{f.name}</div>
                <div className="text-xs text-(--color-text-muted)">
                  {f.deployedVersion != null
                    ? t('flows.deployedVersion', { version: f.deployedVersion })
                    : t('flows.notDeployed')}
                </div>
              </div>
              <Link
                to={`/projects/${projectId}/flows/${f.id}`}
                className="rounded border border-(--color-border) px-2 py-1 text-xs"
              >
                {t('flows.open')}
              </Link>
            </li>
          ))}
        </ul>
      )}

      <form onSubmit={onCreate} className="mb-6 rounded border border-(--color-border) p-4">
        <h2 className="mb-3 text-sm font-semibold">{t('flows.create')}</h2>
        <label className="mb-3 block text-sm">
          {t('flows.name')}
          <input
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </label>
        <button
          type="submit"
          disabled={creating}
          className="rounded bg-(--color-accent) px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          {t('flows.create.submit')}
        </button>
      </form>

      <h2 className="mb-2 text-sm font-semibold text-(--color-text-muted)">{t('connections.title')}</h2>
      {connections === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : connections.length === 0 ? (
        <p className="mb-6 text-sm text-(--color-text-muted)">{t('connections.empty')}</p>
      ) : (
        <ul className="mb-6 divide-y divide-(--color-border) rounded border border-(--color-border)">
          {connections.map((c) => (
            <ConnectionRow key={c.id} connection={c} onDelete={() => void onDeleteConnection(c.id)} />
          ))}
        </ul>
      )}

      {projectId && (
        <CreateConnectionForm
          projectId={projectId}
          onCreated={(c) => setConnections((prev) => [...(prev ?? []), c])}
        />
      )}
    </div>
  )
}

function ConnectionRow({ connection, onDelete }: { connection: Connection; onDelete: () => void }) {
  const { t } = useI18n()
  const [testing, setTesting] = useState(false)
  const [result, setResult] = useState<ConnectionTestResult | null>(null)

  async function onTest() {
    setTesting(true)
    try {
      setResult(await api.testConnection(connection.id))
    } finally {
      setTesting(false)
    }
  }

  return (
    <li className="flex items-center justify-between px-3 py-2">
      <div>
        <div className="text-sm font-medium">{connection.name}</div>
        <div className="text-xs text-(--color-text-muted)">{connection.type}</div>
        {result && (
          <div className={`text-xs ${result.ok ? 'text-green-600' : 'text-red-600'}`}>{result.message}</div>
        )}
      </div>
      <div className="flex gap-2">
        <button
          onClick={() => void onTest()}
          disabled={testing}
          className="rounded border border-(--color-border) px-2 py-1 text-xs disabled:opacity-50"
        >
          {testing ? t('connections.testing') : t('connections.test')}
        </button>
        <button onClick={onDelete} className="rounded border border-(--color-border) px-2 py-1 text-xs">
          {t('connections.delete')}
        </button>
      </div>
    </li>
  )
}

function CreateConnectionForm({
  projectId,
  onCreated,
}: {
  projectId: string
  onCreated: (c: Connection) => void
}) {
  const { t } = useI18n()
  const [name, setName] = useState('')
  const [type, setType] = useState('')
  const [config, setConfig] = useState('{}')
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    let parsedConfig: Record<string, unknown>
    try {
      parsedConfig = JSON.parse(config) as Record<string, unknown>
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return
    }
    setCreating(true)
    try {
      const connection = await api.createConnection(projectId, name, type, parsedConfig)
      onCreated(connection)
      setName('')
      setType('')
      setConfig('{}')
    } finally {
      setCreating(false)
    }
  }

  return (
    <form onSubmit={(e) => void onSubmit(e)} className="rounded border border-(--color-border) p-4">
      <h2 className="mb-3 text-sm font-semibold">{t('connections.create')}</h2>
      <label className="mb-3 block text-sm">
        {t('connections.name')}
        <input
          className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
        />
      </label>
      <label className="mb-3 block text-sm">
        {t('connections.type')}
        <input
          className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
          value={type}
          onChange={(e) => setType(e.target.value)}
          placeholder="mqtt, postgres, …"
          required
        />
      </label>
      <label className="mb-3 block text-sm">
        {t('connections.config')}
        <textarea
          className="mt-1 w-full rounded border border-(--color-border) bg-transparent p-1 font-mono"
          rows={3}
          value={config}
          onChange={(e) => setConfig(e.target.value)}
        />
      </label>
      {error && <p className="mb-3 text-sm text-red-600">{error}</p>}
      <button
        type="submit"
        disabled={creating}
        className="rounded bg-(--color-accent) px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
      >
        {t('connections.create.submit')}
      </button>
    </form>
  )
}
