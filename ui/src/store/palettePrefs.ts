import { create } from 'zustand'

// UI-110: "favorites; recently used" — per-browser preferences, not
// server-side state.
const FAVORITES_KEY = 'datapipe.palette.favorites'
const RECENT_KEY = 'datapipe.palette.recent'
const MAX_RECENT = 8

function readList(key: string): string[] {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as string[]) : []
  } catch {
    return []
  }
}

interface PalettePrefsState {
  favorites: Set<string>
  recent: string[]
  toggleFavorite: (nodeType: string) => void
  recordUsed: (nodeType: string) => void
}

export const usePalettePrefsStore = create<PalettePrefsState>((set, get) => ({
  favorites: new Set(readList(FAVORITES_KEY)),
  recent: readList(RECENT_KEY),

  toggleFavorite: (nodeType) => {
    const favorites = new Set(get().favorites)
    if (favorites.has(nodeType)) favorites.delete(nodeType)
    else favorites.add(nodeType)
    localStorage.setItem(FAVORITES_KEY, JSON.stringify([...favorites]))
    set({ favorites })
  },

  recordUsed: (nodeType) => {
    const recent = [nodeType, ...get().recent.filter((t) => t !== nodeType)].slice(0, MAX_RECENT)
    localStorage.setItem(RECENT_KEY, JSON.stringify(recent))
    set({ recent })
  },
}))
