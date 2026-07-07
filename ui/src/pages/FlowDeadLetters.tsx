import { useEffect, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type { DeadLetter } from '../api/types'
import { JsonTree } from '../components/JsonTree'
import { useI18n } from '../i18n'

// ERR-130: browse a flow's dead letters (undeliverable/expired datagrams)
// and re-inject one after the underlying issue is fixed.
export default function FlowDeadLetters() {
  const { t } = useI18n()
  const { projectId, flowId } = useParams<{ projectId: string; flowId: string }>()
  const [deadLetters, setDeadLetters] = useState<DeadLetter[] | null>(null)

  function reload() {
    if (!flowId) return
    void api.listDeadLetters(flowId).then(setDeadLetters)
  }

  useEffect(reload, [flowId])

  async function onDelete(id: string) {
    await api.deleteDeadLetter(id)
    setDeadLetters((prev) => (prev ?? []).filter((d) => d.id !== id))
  }

  return (
    <div className="mx-auto max-w-3xl p-6">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Link to={`/projects/${projectId}/flows/${flowId}`} className="text-(--color-accent)">
            ←
          </Link>
          <h1 className="text-lg font-semibold">{t('deadLetters.title')}</h1>
        </div>
        <Link to={`/projects/${projectId}/flows/${flowId}/executions`} className="text-sm text-(--color-accent)">
          {t('deadLetters.viewExecutions')}
        </Link>
      </div>

      {deadLetters === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : deadLetters.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('deadLetters.empty')}</p>
      ) : (
        <ul className="flex flex-col gap-3">
          {deadLetters.map((d) => (
            <DeadLetterRow key={d.id} deadLetter={d} onDelete={() => void onDelete(d.id)} onReinjected={reload} />
          ))}
        </ul>
      )}
    </div>
  )
}

function DeadLetterRow({
  deadLetter,
  onDelete,
  onReinjected,
}: {
  deadLetter: DeadLetter
  onDelete: () => void
  onReinjected: () => void
}) {
  const { t } = useI18n()
  const [busy, setBusy] = useState(false)

  async function onReinject() {
    setBusy(true)
    try {
      await api.reinjectDeadLetter(deadLetter.id)
      onReinjected()
    } finally {
      setBusy(false)
    }
  }

  return (
    <li className="rounded border border-(--color-border) p-3 text-sm">
      <div className="mb-2 flex items-center justify-between">
        <div>
          <span className="font-medium">
            {t('deadLetters.node')}: {deadLetter.nodeId}
          </span>
          <span className="ml-2 text-xs text-(--color-text-muted)">
            {t('deadLetters.reason')}: {deadLetter.reason}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {deadLetter.reinjectedAt ? (
            <span className="text-xs text-green-600">{t('deadLetters.reinjectedAt')}</span>
          ) : (
            <button
              onClick={() => void onReinject()}
              disabled={busy}
              className="rounded border border-(--color-border) px-2 py-1 text-xs disabled:opacity-50"
            >
              {busy ? t('deadLetters.reinjecting') : t('deadLetters.reinject')}
            </button>
          )}
          <button onClick={onDelete} className="rounded border border-(--color-border) px-2 py-1 text-xs">
            {t('deadLetters.delete')}
          </button>
        </div>
      </div>
      <JsonTree value={deadLetter.datagram} />
    </li>
  )
}
