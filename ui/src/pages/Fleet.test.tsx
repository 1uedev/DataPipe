import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { I18nProvider } from '../i18n'
import type { RuntimeGroup, RuntimeInfo } from '../api/types'
import { useAuthStore } from '../store/auth'

const listRuntimes = vi.fn()
const listRuntimeGroups = vi.fn()
const listEnrollTokens = vi.fn()
vi.mock('../api/resources', () => ({
  listRuntimes: (...args: unknown[]) => listRuntimes(...args),
  listRuntimeGroups: (...args: unknown[]) => listRuntimeGroups(...args),
  listEnrollTokens: (...args: unknown[]) => listEnrollTokens(...args),
}))

// Imported after the mock so Fleet picks up the mocked module.
const { default: Fleet } = await import('./Fleet')

function renderPage() {
  return render(
    <I18nProvider>
      <MemoryRouter initialEntries={['/fleet']}>
        <Fleet />
      </MemoryRouter>
    </I18nProvider>,
  )
}

const sampleRuntime: RuntimeInfo = {
  runtimeId: 'rt-1',
  kind: 'edge',
  version: '0.0.1',
  lastSeen: '2026-07-07T00:00:00Z',
  online: true,
  cpuPercent: 12.5,
  memoryBytes: 1024 * 1024 * 64,
  flowCount: 2,
  displayName: null,
  group: 'edge-fab2',
  enrolled: true,
}

const sampleGroup: RuntimeGroup = { name: 'edge-fab2', description: 'Fab 2 line', createdAt: '2026-07-07T00:00:00Z' }

describe('Fleet', () => {
  beforeEach(() => {
    listRuntimes.mockReset()
    listRuntimeGroups.mockReset()
    listEnrollTokens.mockReset()
    useAuthStore.setState({ user: null })
  })

  it('renders the empty state when there are no runtimes', async () => {
    listRuntimes.mockResolvedValueOnce([])
    listRuntimeGroups.mockResolvedValueOnce([])
    renderPage()
    await waitFor(() => expect(screen.getByText('No runtimes have registered yet.')).toBeInTheDocument())
  })

  it('renders a fetched runtime with its health and group', async () => {
    listRuntimes.mockResolvedValueOnce([sampleRuntime])
    listRuntimeGroups.mockResolvedValueOnce([sampleGroup])
    renderPage()
    await waitFor(() => expect(screen.getByText('rt-1')).toBeInTheDocument())
    expect(screen.getByText('13%')).toBeInTheDocument()
    expect(screen.getByText('64 MB')).toBeInTheDocument()
  })

  it('hides group/token management for non-System-Admin users', async () => {
    listRuntimes.mockResolvedValueOnce([sampleRuntime])
    listRuntimeGroups.mockResolvedValueOnce([sampleGroup])
    renderPage()
    await waitFor(() => expect(screen.getByText('rt-1')).toBeInTheDocument())
    expect(screen.getByText('Only a System Admin can manage groups, enrollment tokens, and device assignment.')).toBeInTheDocument()
    expect(screen.queryByText('Enrollment tokens')).not.toBeInTheDocument()
    expect(listEnrollTokens).not.toHaveBeenCalled()
  })

  it('shows token management for System Admin users', async () => {
    useAuthStore.setState({
      user: { id: 'u1', username: 'admin', systemRole: 'system_admin', createdAt: '2026-07-07T00:00:00Z' },
    })
    listRuntimes.mockResolvedValueOnce([sampleRuntime])
    listRuntimeGroups.mockResolvedValueOnce([sampleGroup])
    listEnrollTokens.mockResolvedValueOnce([])
    renderPage()
    await waitFor(() => expect(screen.getByText('Enrollment tokens')).toBeInTheDocument())
    expect(listEnrollTokens).toHaveBeenCalled()
  })
})
