import { describe, expect, it } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { I18nProvider } from '../i18n'
import { JsonTree } from './JsonTree'

function renderTree(value: unknown) {
  return render(
    <I18nProvider>
      <JsonTree value={value} />
    </I18nProvider>,
  )
}

describe('JsonTree', () => {
  it('renders top-level keys collapsed by default', () => {
    renderTree({ temp: 42.5, nested: { unit: 'C' } })
    expect(screen.getByText('temp:')).toBeInTheDocument()
    expect(screen.getByText('42.5')).toBeInTheDocument()
    expect(screen.getByText('nested:')).toBeInTheDocument()
    expect(screen.queryByText('unit:')).not.toBeInTheDocument()
  })

  it('expands a nested object on click', () => {
    renderTree({ nested: { unit: 'C' } })
    fireEvent.click(screen.getByLabelText('expand'))
    expect(screen.getByText('unit:')).toBeInTheDocument()
    expect(screen.getByText('C')).toBeInTheDocument()
  })

  it('renders an empty object/array placeholder', () => {
    renderTree({ empty: {} })
    fireEvent.click(screen.getByLabelText('expand'))
    expect(screen.getByText('{}')).toBeInTheDocument()
  })

  it('renders array indices as keys', () => {
    renderTree([1, 2])
    expect(screen.getByText('0:')).toBeInTheDocument()
    expect(screen.getByText('1:')).toBeInTheDocument()
  })

  it('copies a leaf value to the clipboard on click', () => {
    let written = ''
    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: (text: string) => {
          written = text
          return Promise.resolve()
        },
      },
      configurable: true,
    })
    renderTree({ temp: 42.5 })
    fireEvent.click(screen.getByText('42.5'))
    expect(written).toBe('42.5')
  })
})
