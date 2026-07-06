import { useState } from 'react'
import { useI18n } from '../i18n'

// DBG-100: "expandable JSON tree ... copy path / copy value". Clicking a
// key copies its "."-separated path (matching the "set"/"debug-log" nodes'
// own path syntax); clicking a leaf value copies the value itself.
interface JsonTreeProps {
  value: unknown
  path?: string
}

export function JsonTree({ value, path = '' }: JsonTreeProps) {
  if (value === null || typeof value !== 'object') {
    return <JsonLeaf value={value} />
  }
  const entries = Array.isArray(value)
    ? value.map((v, i) => [String(i), v] as const)
    : (Object.entries(value as Record<string, unknown>) as [string, unknown][])

  if (entries.length === 0) {
    return <span className="font-mono text-(--color-text-muted)">{Array.isArray(value) ? '[]' : '{}'}</span>
  }

  return (
    <ul className="flex flex-col gap-0.5 pl-3">
      {entries.map(([key, v]) => (
        <JsonNode key={key} keyName={key} value={v} path={path ? `${path}.${key}` : key} />
      ))}
    </ul>
  )
}

function JsonNode({ keyName, value, path }: { keyName: string; value: unknown; path: string }) {
  const { t } = useI18n()
  const [open, setOpen] = useState(false)
  const [copiedPath, copyPath] = useCopyFlash()
  const expandable = value !== null && typeof value === 'object'

  return (
    <li className="text-xs">
      <div className="flex items-center gap-1">
        {expandable ? (
          <button onClick={() => setOpen((o) => !o)} className="w-3 shrink-0 text-(--color-text-muted)" aria-label={open ? 'collapse' : 'expand'}>
            {open ? '▾' : '▸'}
          </button>
        ) : (
          <span className="w-3 shrink-0" />
        )}
        <button
          onClick={() => copyPath(path)}
          title={t('inspector.copyPath')}
          className="font-mono font-medium text-(--color-text-muted) hover:text-(--color-accent)"
        >
          {keyName}:
        </button>
        {!expandable && <JsonLeaf value={value} />}
        {copiedPath === path && <span className="text-(--color-accent)">{t('inspector.copied')}</span>}
      </div>
      {expandable && open && <JsonTree value={value} path={path} />}
    </li>
  )
}

function JsonLeaf({ value }: { value: unknown }) {
  const { t } = useI18n()
  const [copiedValue, copyValue] = useCopyFlash()
  const text = typeof value === 'string' ? value : JSON.stringify(value)

  return (
    <span className="flex items-center gap-1">
      <button onClick={() => copyValue(text)} title={t('inspector.copyValue')} className="font-mono hover:text-(--color-accent)">
        {text}
      </button>
      {copiedValue === text && <span className="text-(--color-accent)">{t('inspector.copied')}</span>}
    </span>
  )
}

function useCopyFlash(): [string | null, (text: string) => void] {
  const [copied, setCopied] = useState<string | null>(null)
  function copy(text: string) {
    if (typeof navigator !== 'undefined' && navigator.clipboard) {
      void navigator.clipboard.writeText(text)
    }
    setCopied(text)
    setTimeout(() => setCopied((c) => (c === text ? null : c)), 1200)
  }
  return [copied, copy]
}
