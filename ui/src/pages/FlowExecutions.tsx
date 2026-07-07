import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type { Execution, ExecutionStatus } from '../api/types'
import { useI18n } from '../i18n'

// DBG-140: execution history for a triggered flow — browse/filter, newest
// first. Detail (per-node I/O, re-run, cancel) lives in ExecutionDetail.tsx.
const STATUSES: ExecutionStatus[] = ['running', 'waiting', 'success', 'failed', 'cancelled', 'crashed']

const STATUS_COLOR: Record<ExecutionStatus, string> = {
  running: 'text-blue-600',
  waiting: 'text-(--color-text-muted)',
  success: 'text-green-600',
  failed: 'text-red-600',
  cancelled: 'text-(--color-text-muted)',
  crashed: 'text-red-600',
}

export default function FlowExecutions() {
  const { t } = useI18n()
  const { projectId, flowId } = useParams<{ projectId: string; flowId: string }>()
  const [executions, setExecutions] = useState<Execution[] | null>(null)
  const [status, setStatus] = useState<ExecutionStatus | ''>('')

  useEffect(() => {
    if (!flowId) return
    void api.listExecutions(flowId, status || undefined).then(setExecutions)
  }, [flowId, status])

  return (
    <div className="mx-auto max-w-3xl p-6">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Link to={`/projects/${projectId}/flows/${flowId}`} className="text-(--color-accent)">
            ←
          </Link>
          <h1 className="text-lg font-semibold">{t('executions.title')}</h1>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value as ExecutionStatus | '')}
            className="rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
          >
            <option value="">{t('executions.filter.all')}</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {t(`executions.status.${s}`)}
              </option>
            ))}
          </select>
          <Link to={`/projects/${projectId}/flows/${flowId}/dead-letters`} className="text-sm text-(--color-accent)">
            {t('executions.viewDeadLetters')}
          </Link>
        </div>
      </div>

      {executions === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : executions.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('executions.empty')}</p>
      ) : (
        <ul className="divide-y divide-(--color-border) rounded border border-(--color-border)">
          {executions.map((e) => (
            <li key={e.id}>
              <Link
                to={`/projects/${projectId}/flows/${flowId}/executions/${e.id}`}
                className="flex items-center justify-between px-3 py-2 text-sm hover:bg-(--color-bg-subtle)"
              >
                <div>
                  <div className="font-mono text-xs">{e.id}</div>
                  <div className="text-xs text-(--color-text-muted)">
                    {t('executions.trigger', { kind: e.triggerKind })}
                    {e.reRunOf ? ` · ${t('executions.reRunOf', { id: e.reRunOf })}` : ''}
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  {e.durationMs != null && (
                    <span className="text-xs text-(--color-text-muted)">{t('executions.duration', { ms: e.durationMs })}</span>
                  )}
                  <span className={`text-xs font-medium ${STATUS_COLOR[e.status]}`}>{t(`executions.status.${e.status}`)}</span>
                </div>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
