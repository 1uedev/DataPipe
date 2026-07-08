import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import * as api from '../api/resources'
import type { Connection, Flow, Project, RuntimeInfo } from '../api/types'
import { useI18n } from '../i18n'

// OBS-110: built-in monitoring UI — runtime health, flow health (deployed
// status + a recent-failure signal), and a connection status board — no
// external tooling required for basic operation. The edge fleet view
// itself is Fleet.tsx (Increment 9); this page complements it with the
// cross-project flow/connection health OBS-110 also calls for.
export default function Monitoring() {
  const { t } = useI18n()
  const [runtimes, setRuntimes] = useState<RuntimeInfo[] | null>(null)
  const [flowRows, setFlowRows] = useState<FlowHealthRow[] | null>(null)
  const [connRows, setConnRows] = useState<ConnRow[] | null>(null)

  useEffect(() => {
    void api.listRuntimes().then(setRuntimes)
  }, [])

  useEffect(() => {
    void loadFlowHealth().then(setFlowRows)
    void loadConnections().then(setConnRows)
  }, [])

  return (
    <div className="mx-auto max-w-5xl p-6">
      <h1 className="mb-4 text-lg font-semibold">{t('monitoring.title')}</h1>

      <section className="mb-8">
        <h2 className="mb-2 text-sm font-semibold">{t('monitoring.runtimes')}</h2>
        <RuntimeHealthTable runtimes={runtimes} />
      </section>

      <section className="mb-8">
        <h2 className="mb-2 text-sm font-semibold">{t('monitoring.flows')}</h2>
        <FlowHealthTable rows={flowRows} />
      </section>

      <section>
        <h2 className="mb-2 text-sm font-semibold">{t('monitoring.connections')}</h2>
        <ConnectionStatusTable rows={connRows} />
      </section>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MB`
  return `${(bytes / 1024).toFixed(0)} KB`
}

function RuntimeHealthTable({ runtimes }: { runtimes: RuntimeInfo[] | null }) {
  const { t } = useI18n()
  if (runtimes === null) return <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
  if (runtimes.length === 0) return <p className="text-sm text-(--color-text-muted)">{t('fleet.empty')}</p>

  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-(--color-border) text-left text-xs text-(--color-text-muted)">
          <th className="py-2 pr-2">{t('fleet.runtimeId')}</th>
          <th className="py-2 pr-2">{t('monitoring.status')}</th>
          <th className="py-2 pr-2">{t('fleet.cpu')}</th>
          <th className="py-2 pr-2">{t('fleet.memory')}</th>
          <th className="py-2 pr-2">{t('fleet.flows')}</th>
        </tr>
      </thead>
      <tbody>
        {runtimes.map((rt) => (
          <tr key={rt.runtimeId} className="border-b border-(--color-border)">
            <td className="py-2 pr-2 font-mono text-xs">{rt.displayName ? `${rt.displayName} (${rt.runtimeId})` : rt.runtimeId}</td>
            <td className="py-2 pr-2">
              <StatusBadge ok={rt.online} okLabel={t('monitoring.status.online')} badLabel={t('monitoring.status.offline')} />
            </td>
            <td className="py-2 pr-2">{rt.online && rt.cpuPercent != null ? `${rt.cpuPercent.toFixed(0)}%` : '—'}</td>
            <td className="py-2 pr-2">{rt.online && rt.memoryBytes != null ? formatBytes(rt.memoryBytes) : '—'}</td>
            <td className="py-2 pr-2">{rt.flowCount}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function StatusBadge({ ok, okLabel, badLabel }: { ok: boolean; okLabel: string; badLabel: string }) {
  return (
    <span className={ok ? 'text-(--color-accent)' : 'text-(--color-text-muted)'}>
      {ok ? okLabel : badLabel}
    </span>
  )
}

interface FlowHealthRow {
  project: Project
  flow: Flow
  hasRecentFailures: boolean
}

// loadFlowHealth aggregates every project's flows client-side (there is no
// single cross-project "list all flows" endpoint) and, for each deployed
// triggered flow, checks whether it has any recent failed execution — an
// honest "has a problem" signal rather than a precise error-rate number the
// underlying APIs don't return a count for (Executions/DeadLetters return
// bounded pages, not totals).
async function loadFlowHealth(): Promise<FlowHealthRow[]> {
  const projects = await api.listProjects()
  const rows: FlowHealthRow[] = []
  for (const project of projects) {
    const flows = await api.listFlows(project.id)
    for (const flow of flows) {
      let hasRecentFailures = false
      if (flow.deployedVersion != null && flow.content.mode === 'triggered') {
        try {
          const failed = await api.listExecutions(flow.id, 'failed', 1, 0)
          hasRecentFailures = failed.length > 0
        } catch {
          // Best-effort signal only; a lookup failure just means "unknown", not "failed".
        }
      }
      rows.push({ project, flow, hasRecentFailures })
    }
  }
  return rows
}

function FlowHealthTable({ rows }: { rows: FlowHealthRow[] | null }) {
  const { t } = useI18n()
  if (rows === null) return <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
  if (rows.length === 0) return <p className="text-sm text-(--color-text-muted)">{t('monitoring.flows.empty')}</p>

  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-(--color-border) text-left text-xs text-(--color-text-muted)">
          <th className="py-2 pr-2">{t('monitoring.flows.project')}</th>
          <th className="py-2 pr-2">{t('monitoring.flows.flow')}</th>
          <th className="py-2 pr-2">{t('monitoring.status')}</th>
          <th className="py-2 pr-2">{t('monitoring.flows.errors')}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(({ project, flow, hasRecentFailures }) => (
          <tr key={flow.id} className="border-b border-(--color-border)">
            <td className="py-2 pr-2">{project.name}</td>
            <td className="py-2 pr-2">
              <Link to={`/projects/${project.id}/flows/${flow.id}`} className="text-(--color-accent)">
                {flow.name}
              </Link>
            </td>
            <td className="py-2 pr-2">
              <StatusBadge
                ok={flow.deployedVersion != null}
                okLabel={t('monitoring.status.deployed')}
                badLabel={t('monitoring.status.notDeployed')}
              />
            </td>
            <td className="py-2 pr-2">
              {hasRecentFailures ? (
                <Link to={`/projects/${project.id}/flows/${flow.id}/executions`} className="text-red-500">
                  {t('monitoring.flows.hasFailures')}
                </Link>
              ) : (
                <span className="text-(--color-text-muted)">{t('monitoring.flows.noFailures')}</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

interface ConnRow {
  project: Project
  connection: Connection
}

async function loadConnections(): Promise<ConnRow[]> {
  const projects = await api.listProjects()
  const rows: ConnRow[] = []
  for (const project of projects) {
    const connections = await api.listConnections(project.id)
    for (const connection of connections) {
      rows.push({ project, connection })
    }
  }
  return rows
}

function ConnectionStatusTable({ rows }: { rows: ConnRow[] | null }) {
  const { t } = useI18n()
  const [results, setResults] = useState<Record<string, { ok: boolean; message: string } | 'testing'>>({})

  if (rows === null) return <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
  if (rows.length === 0) return <p className="text-sm text-(--color-text-muted)">{t('monitoring.connections.empty')}</p>

  async function onTest(connectionId: string) {
    setResults((r) => ({ ...r, [connectionId]: 'testing' }))
    const result = await api.testConnection(connectionId)
    setResults((r) => ({ ...r, [connectionId]: result }))
  }

  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-(--color-border) text-left text-xs text-(--color-text-muted)">
          <th className="py-2 pr-2">{t('monitoring.flows.project')}</th>
          <th className="py-2 pr-2">{t('connections.name')}</th>
          <th className="py-2 pr-2">{t('connections.type')}</th>
          <th className="py-2 pr-2">{t('monitoring.status')}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(({ project, connection }) => {
          const result = results[connection.id]
          return (
            <tr key={connection.id} className="border-b border-(--color-border)">
              <td className="py-2 pr-2">{project.name}</td>
              <td className="py-2 pr-2">{connection.name}</td>
              <td className="py-2 pr-2 font-mono text-xs">{connection.type}</td>
              <td className="py-2 pr-2">
                {result === 'testing' ? (
                  <span className="text-(--color-text-muted)">{t('connections.testing')}</span>
                ) : result ? (
                  <span className={result.ok ? 'text-(--color-accent)' : 'text-red-500'}>{result.message}</span>
                ) : (
                  <button onClick={() => void onTest(connection.id)} className="rounded border border-(--color-border) px-1.5 py-0.5 text-xs">
                    {t('connections.test')}
                  </button>
                )}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}
