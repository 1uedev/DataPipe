import { useEffect, useRef, useState, type ChangeEvent, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type {
  Connection,
  ConnectionTestResult,
  EnvironmentProfile,
  Flow,
  FlowExportBundle,
  ImportResult,
  Project,
} from '../api/types'
import { useI18n } from '../i18n'
import { templates } from '../templates'
import { downloadJSON } from '../utils/download'
import { emptyFlowContent } from '../utils/flowFile'

export default function ProjectDetail() {
  const { t } = useI18n()
  const { projectId } = useParams<{ projectId: string }>()
  const [project, setProject] = useState<Project | null>(null)
  const [flows, setFlows] = useState<Flow[] | null>(null)
  const [name, setName] = useState('')
  const [creating, setCreating] = useState(false)
  const [connections, setConnections] = useState<Connection[] | null>(null)
  const [importResult, setImportResult] = useState<ImportResult | null>(null)
  const [importError, setImportError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [profiles, setProfiles] = useState<EnvironmentProfile[] | null>(null)

  useEffect(() => {
    if (!projectId) return
    void api.getProject(projectId).then(setProject)
    void api.listFlows(projectId).then((data) => setFlows(data ?? []))
    void api.listConnections(projectId).then((data) => setConnections(data ?? []))
    void api.listEnvProfiles(projectId).then((data) => setProfiles(data ?? []))
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

  async function onUseTemplate(templateId: string) {
    if (!projectId) return
    const template = templates.find((tpl) => tpl.id === templateId)
    if (!template) return
    const result = await api.importProject(projectId, template.bundle)
    setFlows((prev) => [...(prev ?? []), ...result.flows])
    setConnections((prev) => [...(prev ?? []), ...result.connectionsCreated])
  }

  async function onDeleteProfile(id: string) {
    await api.deleteEnvProfile(id)
    setProfiles((prev) => (prev ?? []).filter((p) => p.id !== id))
  }

  async function onExportProject() {
    if (!projectId || !project) return
    const bundle = await api.exportProject(projectId)
    downloadJSON(`${project.name}.project.json`, bundle)
  }

  async function onImportFile(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    e.target.value = ''
    if (!file || !projectId) return
    setImportError(null)
    setImportResult(null)
    let bundle: FlowExportBundle
    try {
      bundle = JSON.parse(await file.text()) as FlowExportBundle
    } catch (err) {
      setImportError(err instanceof Error ? err.message : String(err))
      return
    }
    try {
      const result = await api.importProject(projectId, bundle)
      setImportResult(result)
      setFlows((prev) => [...(prev ?? []), ...result.flows])
      setConnections((prev) => [...(prev ?? []), ...result.connectionsCreated])
    } catch (err) {
      setImportError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <Link to="/projects" className="text-sm text-(--color-accent)">
        ← {t('flows.backToProjects')}
      </Link>
      <div className="mt-2 mb-4 flex items-center justify-between">
        <h1 className="text-xl font-semibold">{project?.name ?? t('common.loading')}</h1>
        <div className="flex gap-2">
          <button onClick={() => void onExportProject()} className="rounded border border-(--color-border) px-2 py-1 text-xs">
            {t('vcs.export')}
          </button>
          <button
            onClick={() => fileInputRef.current?.click()}
            className="rounded border border-(--color-border) px-2 py-1 text-xs"
          >
            {t('vcs.import')}
          </button>
          <input ref={fileInputRef} type="file" accept="application/json" className="hidden" onChange={(e) => void onImportFile(e)} />
        </div>
      </div>

      {importError && <p className="mb-4 text-sm text-red-600">{importError}</p>}
      {importResult && (
        <p className="mb-4 text-sm text-(--color-text-muted)">
          {t('vcs.import.summary', {
            flows: String(importResult.flows.length),
            matched: String(importResult.connectionsMatched.length),
            created: String(importResult.connectionsCreated.length),
          })}
        </p>
      )}

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

      <h2 className="mb-2 text-sm font-semibold text-(--color-text-muted)">{t('templates.title')}</h2>
      <ul className="mb-6 grid grid-cols-1 gap-2 sm:grid-cols-2">
        {templates.map((tpl) => (
          <li key={tpl.id} className="flex flex-col rounded border border-(--color-border) p-3">
            <div className="text-sm font-medium">{tpl.name}</div>
            <div className="mt-1 mb-2 flex-1 text-xs text-(--color-text-muted)">{tpl.description}</div>
            <button
              onClick={() => void onUseTemplate(tpl.id)}
              className="self-start rounded border border-(--color-border) px-2 py-1 text-xs"
            >
              {t('templates.use')}
            </button>
          </li>
        ))}
      </ul>

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

      <h2 className="mt-8 mb-2 text-sm font-semibold text-(--color-text-muted)">{t('profiles.title')}</h2>
      {profiles === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : profiles.length === 0 ? (
        <p className="mb-6 text-sm text-(--color-text-muted)">{t('profiles.empty')}</p>
      ) : (
        <ul className="mb-6 divide-y divide-(--color-border) rounded border border-(--color-border)">
          {profiles.map((p) => (
            <ProfileRow
              key={p.id}
              profile={p}
              onUpdated={(updated) => setProfiles((prev) => (prev ?? []).map((x) => (x.id === updated.id ? updated : x)))}
              onDelete={() => void onDeleteProfile(p.id)}
            />
          ))}
        </ul>
      )}

      {projectId && (
        <CreateProfileForm
          projectId={projectId}
          onCreated={(p) => setProfiles((prev) => [...(prev ?? []), p])}
        />
      )}
    </div>
  )
}

function ProfileRow({
  profile,
  onUpdated,
  onDelete,
}: {
  profile: EnvironmentProfile
  onUpdated: (p: EnvironmentProfile) => void
  onDelete: () => void
}) {
  const { t } = useI18n()
  const [editing, setEditing] = useState(false)
  const [values, setValues] = useState(() => JSON.stringify(profile.values, null, 2))
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  async function onSave() {
    setError(null)
    let parsed: Record<string, string>
    try {
      parsed = JSON.parse(values) as Record<string, string>
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return
    }
    setSaving(true)
    try {
      onUpdated(await api.updateEnvProfile(profile.id, parsed))
      setEditing(false)
    } finally {
      setSaving(false)
    }
  }

  return (
    <li className="px-3 py-2">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium">{profile.name}</div>
        <div className="flex gap-2">
          <button onClick={() => setEditing((v) => !v)} className="rounded border border-(--color-border) px-2 py-1 text-xs">
            {editing ? t('profiles.cancel') : t('profiles.edit')}
          </button>
          <button onClick={onDelete} className="rounded border border-(--color-border) px-2 py-1 text-xs">
            {t('connections.delete')}
          </button>
        </div>
      </div>
      {editing ? (
        <div className="mt-2">
          <textarea
            className="w-full rounded border border-(--color-border) bg-transparent p-1 font-mono text-xs"
            rows={4}
            value={values}
            onChange={(e) => setValues(e.target.value)}
          />
          {error && <p className="mt-1 text-xs text-red-600">{error}</p>}
          <button
            onClick={() => void onSave()}
            disabled={saving}
            className="mt-2 rounded bg-(--color-accent) px-3 py-1 text-xs font-medium text-white disabled:opacity-50"
          >
            {t('profiles.save')}
          </button>
        </div>
      ) : (
        <div className="mt-1 text-xs text-(--color-text-muted)">
          {Object.keys(profile.values).length === 0
            ? t('profiles.empty.values')
            : Object.entries(profile.values)
                .map(([k, v]) => `${k}=${v}`)
                .join(', ')}
        </div>
      )}
    </li>
  )
}

function CreateProfileForm({ projectId, onCreated }: { projectId: string; onCreated: (p: EnvironmentProfile) => void }) {
  const { t } = useI18n()
  const [name, setName] = useState('')
  const [values, setValues] = useState('{}')
  const [error, setError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    let parsed: Record<string, string>
    try {
      parsed = JSON.parse(values) as Record<string, string>
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return
    }
    setCreating(true)
    try {
      const profile = await api.createEnvProfile(projectId, name, parsed)
      onCreated(profile)
      setName('')
      setValues('{}')
    } finally {
      setCreating(false)
    }
  }

  return (
    <form onSubmit={(e) => void onSubmit(e)} className="rounded border border-(--color-border) p-4">
      <h2 className="mb-3 text-sm font-semibold">{t('profiles.create')}</h2>
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
        {t('profiles.values')}
        <textarea
          className="mt-1 w-full rounded border border-(--color-border) bg-transparent p-1 font-mono"
          rows={3}
          value={values}
          onChange={(e) => setValues(e.target.value)}
        />
      </label>
      {error && <p className="mb-3 text-sm text-red-600">{error}</p>}
      <button
        type="submit"
        disabled={creating}
        className="rounded bg-(--color-accent) px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
      >
        {t('profiles.create.submit')}
      </button>
    </form>
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
