import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { I18nProvider } from '../i18n'
import type { Execution } from '../api/types'

const listExecutions = vi.fn()
vi.mock('../api/resources', () => ({
  listExecutions: (...args: unknown[]) => listExecutions(...args),
}))

// Imported after the mock so FlowExecutions picks up the mocked module.
const { default: FlowExecutions } = await import('./FlowExecutions')

function renderPage() {
  return render(
    <I18nProvider>
      <MemoryRouter initialEntries={['/projects/p1/flows/f1/executions']}>
        <Routes>
          <Route path="/projects/:projectId/flows/:flowId/executions" element={<FlowExecutions />} />
        </Routes>
      </MemoryRouter>
    </I18nProvider>,
  )
}

const sampleExecution: Execution = {
  id: 'exec-1',
  flowId: 'f1',
  runtimeId: 'rt-1',
  status: 'failed',
  triggerNodeId: 'trig',
  triggerKind: 'webhook',
  reRunOf: null,
  startedAt: '2026-07-07T00:00:00Z',
  finishedAt: '2026-07-07T00:00:01Z',
  durationMs: 1000,
  reason: 'boom',
}

describe('FlowExecutions', () => {
  it('renders the empty state when there are no executions', async () => {
    listExecutions.mockResolvedValueOnce([])
    renderPage()
    await waitFor(() => expect(screen.getByText('No executions yet.')).toBeInTheDocument())
    expect(listExecutions).toHaveBeenCalledWith('f1', undefined)
  })

  it('renders a fetched execution with its status and duration', async () => {
    listExecutions.mockResolvedValueOnce([sampleExecution])
    renderPage()
    await waitFor(() => expect(screen.getByText('exec-1')).toBeInTheDocument())
    // "Failed" also appears as a <select> option, so assert on the status link.
    expect(screen.getByRole('link', { name: /Failed/ })).toBeInTheDocument()
    expect(screen.getByText('1000 ms')).toBeInTheDocument()
  })
})
