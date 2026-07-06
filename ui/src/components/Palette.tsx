import { useMemo, useState } from 'react'
import type { NodeType } from '../api/types'
import { usePalettePrefsStore } from '../store/palettePrefs'
import { useI18n } from '../i18n'

interface PaletteProps {
  nodeTypes: NodeType[]
}

const CATEGORY_ORDER: NodeType['category'][] = ['source', 'processor', 'sink', 'control']

// UI-110: "categorized, searchable node library (search by name,
// description, protocol); favorites; recently used; drag onto canvas to
// instantiate."
export function Palette({ nodeTypes }: PaletteProps) {
  const { t } = useI18n()
  const [query, setQuery] = useState('')
  const favorites = usePalettePrefsStore((s) => s.favorites)
  const recent = usePalettePrefsStore((s) => s.recent)
  const toggleFavorite = usePalettePrefsStore((s) => s.toggleFavorite)

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return nodeTypes
    return nodeTypes.filter(
      (n) => n.displayName.toLowerCase().includes(q) || n.description.toLowerCase().includes(q) || n.type.includes(q),
    )
  }, [nodeTypes, query])

  const byCategory = useMemo(() => {
    const groups = new Map<string, NodeType[]>()
    for (const n of filtered) {
      const list = groups.get(n.category) ?? []
      list.push(n)
      groups.set(n.category, list)
    }
    return groups
  }, [filtered])

  const recentTypes = useMemo(
    () => recent.map((type) => nodeTypes.find((n) => n.type === type)).filter((n): n is NodeType => Boolean(n)),
    [recent, nodeTypes],
  )

  function onDragStart(e: React.DragEvent, nodeType: string) {
    e.dataTransfer.setData('application/datapipe-node-type', nodeType)
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <aside className="flex h-full w-64 flex-col border-r border-(--color-border) bg-(--color-bg)">
      <div className="border-b border-(--color-border) p-2">
        <h2 className="mb-2 text-sm font-semibold">{t('editor.palette.title')}</h2>
        <input
          className="w-full rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
          placeholder={t('editor.palette.search')}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </div>
      <div className="flex-1 overflow-y-auto p-2">
        {!query && recentTypes.length > 0 && (
          <PaletteGroup
            title={t('editor.palette.recent')}
            items={recentTypes}
            favorites={favorites}
            onToggleFavorite={toggleFavorite}
            onDragStart={onDragStart}
          />
        )}
        {filtered.length === 0 ? (
          <p className="p-2 text-sm text-(--color-text-muted)">{t('editor.palette.empty')}</p>
        ) : (
          CATEGORY_ORDER.map((category) => {
            const items = byCategory.get(category)
            if (!items || items.length === 0) return null
            return (
              <PaletteGroup
                key={category}
                title={category}
                items={items}
                favorites={favorites}
                onToggleFavorite={toggleFavorite}
                onDragStart={onDragStart}
              />
            )
          })
        )}
      </div>
    </aside>
  )
}

function PaletteGroup({
  title,
  items,
  favorites,
  onToggleFavorite,
  onDragStart,
}: {
  title: string
  items: NodeType[]
  favorites: Set<string>
  onToggleFavorite: (type: string) => void
  onDragStart: (e: React.DragEvent, type: string) => void
}) {
  const { t } = useI18n()
  return (
    <div className="mb-3">
      <h3 className="mb-1 px-1 text-xs font-semibold tracking-wide text-(--color-text-muted) uppercase">{title}</h3>
      <ul>
        {items.map((n) => (
          <li
            key={n.type}
            draggable
            onDragStart={(e) => onDragStart(e, n.type)}
            className="flex cursor-grab items-center justify-between rounded px-2 py-1.5 text-sm hover:bg-(--color-bg-subtle)"
            title={n.description}
          >
            <span>{n.displayName}</span>
            <button
              onClick={() => onToggleFavorite(n.type)}
              aria-label={t('editor.palette.favorite')}
              className={favorites.has(n.type) ? 'text-yellow-500' : 'text-(--color-text-muted)'}
            >
              {favorites.has(n.type) ? '★' : '☆'}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
