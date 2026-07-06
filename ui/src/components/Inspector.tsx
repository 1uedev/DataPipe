import { useEffect, useState } from 'react'
import * as api from '../api/resources'
import type { DebugEvent, DebugPin, ExecuteNodeResult } from '../api/types'
import { EMPTY_EVENTS, useDebugStore } from '../store/debug'
import { JsonTree } from './JsonTree'
import { useI18n } from '../i18n'

// DBG-100 (live ring buffer) + DBG-130 (design-time single-node execution
// and data pinning), shown as the ConfigPanel's "Inspect" tab.
interface InspectorProps {
  flowId: string
  nodeId: string
}

export function Inspector({ flowId, nodeId }: InspectorProps) {
  const { t } = useI18n()
  const events = useDebugStore((s) => s.nodeEvents[nodeId] ?? EMPTY_EVENTS)
  const [viewMode, setViewMode] = useState<'tree' | 'raw'>('tree')
  const [runPayload, setRunPayload] = useState('{}')
  const [runError, setRunError] = useState<string | null>(null)
  const [runResult, setRunResult] = useState<ExecuteNodeResult | null>(null)
  const [running, setRunning] = useState(false)
  const [pins, setPins] = useState<DebugPin[]>([])

  useEffect(() => {
    let cancelled = false
    void api.listPins(flowId).then((all) => {
      if (!cancelled) setPins(all.filter((p) => p.nodeId === nodeId))
    })
    return () => {
      cancelled = true
    }
  }, [flowId, nodeId])

  async function onRun() {
    setRunning(true)
    setRunError(null)
    try {
      const payload: unknown = JSON.parse(runPayload)
      const result = await api.executeNode(flowId, nodeId, payload)
      setRunResult(result)
    } catch (err) {
      setRunError(err instanceof Error ? err.message : String(err))
      setRunResult(null)
    } finally {
      setRunning(false)
    }
  }

  async function onPin(port: string, value: unknown) {
    const pin = await api.setPin(flowId, nodeId, port, value)
    setPins((prev) => [...prev.filter((p) => p.port !== port), pin])
  }

  async function onUnpin(port: string) {
    await api.deletePin(flowId, nodeId, port)
    setPins((prev) => prev.filter((p) => p.port !== port))
  }

  return (
    <div className="flex flex-col gap-4 text-xs">
      <section>
        <h3 className="font-semibold">{t('inspector.run.title')}</h3>
        <label className="mt-1 block">
          <span className="text-(--color-text-muted)">{t('inspector.run.payload')}</span>
          <textarea
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent p-1 font-mono"
            rows={3}
            value={runPayload}
            onChange={(e) => setRunPayload(e.target.value)}
          />
        </label>
        <button
          onClick={() => void onRun()}
          disabled={running}
          className="mt-1 rounded border border-(--color-border) px-2 py-1 disabled:opacity-50"
        >
          {running ? t('inspector.run.running') : t('inspector.run.button')}
        </button>

        {runError && <p className="mt-2 text-red-600">{runError}</p>}
        {runResult?.error && <p className="mt-2 text-red-600">{runResult.error}</p>}
        {runResult && !runResult.error && (
          <div className="mt-2 flex flex-col gap-1">
            {runResult.outputs.length === 0 && <p className="text-(--color-text-muted)">{t('inspector.live.empty')}</p>}
            {runResult.outputs.map((o, i) => (
              <div key={`${o.port}-${i}`} className="rounded border border-(--color-border) p-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-mono font-medium">{o.port}</span>
                  <button onClick={() => void onPin(o.port, o.datagram)} className="text-(--color-accent)">
                    {t('inspector.pin')}
                  </button>
                </div>
                <JsonTree value={o.datagram} />
              </div>
            ))}
          </div>
        )}
      </section>

      {pins.length > 0 && (
        <section>
          <h3 className="font-semibold">{t('inspector.pins.title')}</h3>
          <div className="mt-1 flex flex-col gap-1">
            {pins.map((p) => (
              <div key={p.port} className="flex items-center justify-between rounded border border-(--color-border) p-1.5">
                <span className="font-mono">{p.port}</span>
                <button onClick={() => void onUnpin(p.port)} className="text-(--color-text-muted) hover:text-(--color-accent)">
                  {t('inspector.unpin')}
                </button>
              </div>
            ))}
          </div>
        </section>
      )}

      <section>
        <div className="flex items-center justify-between">
          <h3 className="font-semibold">{t('inspector.live.title')}</h3>
          <div className="flex gap-2">
            <button
              onClick={() => setViewMode('tree')}
              className={viewMode === 'tree' ? 'font-semibold text-(--color-accent)' : 'text-(--color-text-muted)'}
            >
              {t('inspector.view.tree')}
            </button>
            <button
              onClick={() => setViewMode('raw')}
              className={viewMode === 'raw' ? 'font-semibold text-(--color-accent)' : 'text-(--color-text-muted)'}
            >
              {t('inspector.view.raw')}
            </button>
          </div>
        </div>
        {events.length === 0 ? (
          <p className="mt-1 text-(--color-text-muted)">{t('inspector.live.empty')}</p>
        ) : (
          <ul className="mt-1 flex flex-col gap-1">
            {[...events].reverse().map((e) => (
              <DebugEventRow key={e.id} event={e} flowId={flowId} viewMode={viewMode} />
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}

function parseValueJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function DebugEventRow({ event, flowId, viewMode }: { event: DebugEvent; flowId: string; viewMode: 'tree' | 'raw' }) {
  const { t } = useI18n()
  const [full, setFull] = useState<string | null>(null)
  const valueJson = full ?? event.valueJson
  const parsed = parseValueJson(valueJson)

  async function loadFull() {
    const res = await api.loadFullDebugEvent(flowId, event.id)
    setFull(res.valueJson)
  }

  return (
    <li className="rounded border border-(--color-border) p-1.5">
      <div className="flex items-center justify-between text-(--color-text-muted)">
        <span>
          {event.direction} · {event.port} · {event.quality}
        </span>
        <span>{new Date(event.timeUnixMs).toLocaleTimeString()}</span>
      </div>
      <div className="mt-1">
        {viewMode === 'tree' ? (
          <JsonTree value={parsed} />
        ) : (
          <pre className="overflow-x-auto whitespace-pre-wrap font-mono">{JSON.stringify(parsed, null, 2)}</pre>
        )}
      </div>
      {event.truncated && !full && (
        <button onClick={() => void loadFull()} className="mt-1 text-(--color-accent)">
          {t('inspector.loadFull')} ({event.fullLength} B)
        </button>
      )}
    </li>
  )
}
