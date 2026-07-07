import { useCallback, useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type { ExecutionDetail as ExecutionDetailType, ExecutionStatus } from '../api/types'
import { JsonTree } from '../components/JsonTree'
import { useI18n } from '../i18n'

const STATUS_COLOR: Record<ExecutionStatus, string> = {
  running: 'text-blue-600',
  waiting: 'text-(--color-text-muted)',
  success: 'text-green-600',
  failed: 'text-red-600',
  cancelled: 'text-(--color-text-muted)',
  crashed: 'text-red-600',
}

// DBG-140 execution detail: full per-node input/output trace, plus
// ENG-130's re-run-from-start / re-run-from-failed-node / cancel actions.
export default function ExecutionDetail() {
  const { t } = useI18n()
  const { projectId, flowId, executionId } = useParams<{ projectId: string; flowId: string; executionId: string }>()
  const [execution, setExecution] = useState<ExecutionDetailType | null>(null)
  const [command, setCommand] = useState<'idle' | 'running' | 'accepted' | 'error'>('idle')

  const reload = useCallback(() => {
    if (!executionId) return
    void api.getExecution(executionId).then(setExecution)
  }, [executionId])

  useEffect(() => {
    reload()
  }, [reload])

  async function onRerun(from: 'start' | 'node', nodeId?: string) {
    if (!executionId) return
    setCommand('running')
    try {
      await api.rerunExecution(executionId, from, nodeId)
      setCommand('accepted')
    } catch {
      setCommand('error')
    }
  }

  async function onCancel() {
    if (!executionId) return
    setCommand('running')
    try {
      await api.cancelExecution(executionId)
      setCommand('accepted')
      reload()
    } catch {
      setCommand('error')
    }
  }

  if (!execution) {
    return <div className="p-6 text-sm text-(--color-text-muted)">{t('common.loading')}</div>
  }

  const canCancel = execution.status === 'running' || execution.status === 'waiting'

  return (
    <div className="mx-auto max-w-3xl p-6">
      <div className="mb-4 flex items-center gap-2">
        <Link to={`/projects/${projectId}/flows/${flowId}/executions`} className="text-(--color-accent)">
          ←
        </Link>
        <h1 className="text-lg font-semibold">{t('executions.detail.title', { id: execution.id })}</h1>
        <span className={`text-sm font-medium ${STATUS_COLOR[execution.status]}`}>{t(`executions.status.${execution.status}`)}</span>
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-3 text-sm">
        <span className="text-(--color-text-muted)">{t('executions.trigger', { kind: execution.triggerKind })}</span>
        {execution.durationMs != null && (
          <span className="text-(--color-text-muted)">{t('executions.duration', { ms: execution.durationMs })}</span>
        )}
        {execution.reason && (
          <span className="text-red-600">
            {t('executions.detail.reason')}: {execution.reason}
          </span>
        )}
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-2">
        <button
          onClick={() => void onRerun('start')}
          disabled={command === 'running'}
          className="rounded border border-(--color-border) px-2 py-1 text-xs disabled:opacity-50"
        >
          {command === 'running' ? t('executions.detail.rerunning') : t('executions.detail.rerunFromStart')}
        </button>
        {canCancel && (
          <button
            onClick={() => void onCancel()}
            disabled={command === 'running'}
            className="rounded border border-(--color-border) px-2 py-1 text-xs disabled:opacity-50"
          >
            {command === 'running' ? t('executions.detail.cancelling') : t('executions.detail.cancel')}
          </button>
        )}
        {command === 'accepted' && <span className="text-xs text-green-600">{t('executions.detail.commandAccepted')}</span>}
        {command === 'error' && <span className="text-xs text-red-600">{t('executions.detail.commandFailed')}</span>}
      </div>

      <h2 className="mb-2 text-sm font-semibold">{t('executions.detail.nodeIO')}</h2>
      {execution.nodeIO.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('executions.empty')}</p>
      ) : (
        <ul className="flex flex-col gap-3">
          {execution.nodeIO.map((io, i) => (
            <li key={`${io.nodeId}-${io.attempt}-${i}`} className="rounded border border-(--color-border) p-3 text-sm">
              <div className="mb-2 flex items-center justify-between">
                <div className="font-medium">
                  {io.nodeId}
                  {io.attempt > 1 && (
                    <span className="ml-2 text-xs text-(--color-text-muted)">{t('executions.detail.attempt', { n: io.attempt })}</span>
                  )}
                </div>
                <button
                  onClick={() => void onRerun('node', io.nodeId)}
                  disabled={command === 'running'}
                  className="rounded border border-(--color-border) px-2 py-1 text-xs disabled:opacity-50"
                >
                  {t('executions.detail.rerunFromNode')}
                </button>
              </div>
              {io.error && (
                <div className="mb-2 rounded bg-red-50 px-2 py-1 text-xs text-red-800">
                  {t('executions.detail.error')}: {io.error.message}
                  {io.error.code ? ` (${io.error.code})` : ''}
                </div>
              )}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <div className="mb-1 text-xs font-medium text-(--color-text-muted)">{t('executions.detail.input')}</div>
                  <JsonTree value={io.input} />
                </div>
                <div>
                  <div className="mb-1 text-xs font-medium text-(--color-text-muted)">{t('executions.detail.outputs')}</div>
                  <JsonTree value={io.outputs} />
                </div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
