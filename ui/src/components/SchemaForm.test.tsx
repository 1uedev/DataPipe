import { describe, expect, it, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { I18nProvider } from '../i18n'
import { SchemaForm } from './SchemaForm'
import type { JsonSchema } from '../api/types'

function renderForm(schema: JsonSchema, value: unknown, onChange: (v: unknown) => void) {
  return render(
    <I18nProvider>
      <SchemaForm schema={schema} value={value} onChange={onChange} />
    </I18nProvider>,
  )
}

describe('SchemaForm', () => {
  it('renders object properties and reports changes without mutating the original', () => {
    const schema: JsonSchema = {
      type: 'object',
      properties: {
        label: { type: 'string', description: 'A label' },
      },
      required: ['label'],
    }
    const original = { label: 'demo' }
    const onChange = vi.fn()
    renderForm(schema, original, onChange)

    const input = screen.getByDisplayValue('demo')
    fireEvent.change(input, { target: { value: 'changed' } })

    expect(onChange).toHaveBeenCalledWith({ label: 'changed' })
    expect(original.label).toBe('demo') // the input value itself is untouched
  })

  it('marks required string fields with an inline error when empty', () => {
    const schema: JsonSchema = {
      type: 'object',
      properties: { name: { type: 'string' } },
      required: ['name'],
    }
    renderForm(schema, { name: '' }, vi.fn())
    expect(screen.getAllByText('Required').length).toBeGreaterThan(0)
  })

  it('array field: add/remove items produces the right array', () => {
    const schema: JsonSchema = {
      type: 'array',
      items: {
        type: 'object',
        properties: { path: { type: 'string' }, value: {} },
        required: ['path'],
      },
    }
    const onChange = vi.fn()
    renderForm(schema, [], onChange)

    fireEvent.click(screen.getByText('+'))
    expect(onChange).toHaveBeenCalledWith([{}])
  })

  it('untyped field defaults to literal mode and parses JSON values', () => {
    const schema: JsonSchema = {}
    const onChange = vi.fn()
    renderForm(schema, { value: 21 }, onChange)

    const input = screen.getByDisplayValue('{"value":21}')
    fireEvent.change(input, { target: { value: '{"value":42}' } })
    expect(onChange).toHaveBeenCalledWith({ value: 42 })
  })

  it('untyped field switches to expression mode and wraps the value', () => {
    const onChange = vi.fn()
    renderForm({}, 1, onChange)

    fireEvent.click(screen.getByText('Expression'))
    const exprInput = screen.getByRole('textbox')
    fireEvent.change(exprInput, { target: { value: 'payload.value * 2' } })

    expect(onChange).toHaveBeenCalledWith('={{payload.value * 2}}')
  })
})
