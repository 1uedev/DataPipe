import { useMemo, useState } from 'react'
import { useDebugStore } from '../store/debug'
import { useI18n } from '../i18n'

// DBG-110: "explicit node printing selected expressions to a global debug
// sidebar with filtering by flow/node, pause/clear, and payload size
// truncation with 'load full' on demand." Filtering by flow is implicit —
// one flow is subscribed at a time (FlowEditor's debug connection) — so
// this only needs a node filter on top of that.
export function DebugSidebar({ onClose }: { onClose: () => void }) {
  const { t } = useI18n()
  const events = useDebugStore((s) => s.sidebarEvents)
  const paused = useDebugStore((s) => s.sidebarPaused)
  const togglePaused = useDebugStore((s) => s.toggleSidebarPaused)
  const clear = useDebugStore((s) => s.clearSidebar)
  const [nodeFilter, setNodeFilter] = useState('')

  const nodeIds = useMemo(() => Array.from(new Set(events.map((e) => e.nodeId))).sort(), [events])
  const filtered = nodeFilter ? events.filter((e) => e.nodeId === nodeFilter) : events

  return (
    <aside className="flex h-48 flex-col border-t border-(--color-border) bg-(--color-bg)">
      <div className="flex items-center justify-between border-b border-(--color-border) px-3 py-1.5">
        <h2 className="text-sm font-semibold">{t('debugSidebar.title')}</h2>
        <div className="flex items-center gap-2 text-xs">
          {nodeIds.length > 0 && (
            <select
              value={nodeFilter}
              onChange={(e) => setNodeFilter(e.target.value)}
              className="rounded border border-(--color-border) bg-transparent px-1 py-0.5"
            >
              <option value="">{t('debugSidebar.filterAll')}</option>
              {nodeIds.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          )}
          <button onClick={togglePaused} className="rounded border border-(--color-border) px-2 py-0.5">
            {paused ? t('debugSidebar.resume') : t('debugSidebar.pause')}
          </button>
          <button onClick={clear} className="rounded border border-(--color-border) px-2 py-0.5">
            {t('debugSidebar.clear')}
          </button>
          <button onClick={onClose} aria-label={t('common.close')}>
            ✕
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-2 text-xs">
        {filtered.length === 0 ? (
          <p className="text-(--color-text-muted)">{t('debugSidebar.empty')}</p>
        ) : (
          <ul className="flex flex-col gap-1">
            {[...filtered].reverse().map((e) => (
              <li key={e.id} className="rounded border border-(--color-border) p-1">
                <div className="flex items-center justify-between text-(--color-text-muted)">
                  <span className="font-medium text-(--color-text)">{e.label || e.nodeId}</span>
                  <span>{new Date(e.timeUnixMs).toLocaleTimeString()}</span>
                </div>
                <pre className="overflow-x-auto whitespace-pre-wrap font-mono">{e.valueJson}</pre>
              </li>
            ))}
          </ul>
        )}
      </div>
    </aside>
  )
}
