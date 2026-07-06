import { create } from 'zustand'
import { getToken, setToken } from '../api/token'
import * as api from '../api/resources'
import type { User } from '../api/types'

type Status = 'checking' | 'authenticated' | 'unauthenticated'

interface AuthState {
  status: Status
  user: User | null
  error: string | null
  checkSession: () => Promise<void>
  login: (username: string, password: string) => Promise<void>
  logout: () => Promise<void>
}

export const useAuthStore = create<AuthState>((set) => ({
  status: 'checking',
  user: null,
  error: null,

  checkSession: async () => {
    if (!getToken()) {
      set({ status: 'unauthenticated', user: null })
      return
    }
    try {
      const user = await api.me()
      set({ status: 'authenticated', user })
    } catch {
      setToken(null)
      set({ status: 'unauthenticated', user: null })
    }
  },

  login: async (username, password) => {
    set({ error: null })
    try {
      const { token } = await api.login(username, password)
      setToken(token)
      const user = await api.me()
      set({ status: 'authenticated', user })
    } catch (err) {
      set({ error: err instanceof Error ? err.message : String(err) })
      throw err
    }
  },

  logout: async () => {
    try {
      await api.logout()
    } catch {
      // Best-effort: clear local state regardless of server-side outcome.
    }
    setToken(null)
    set({ status: 'unauthenticated', user: null })
  },
}))
