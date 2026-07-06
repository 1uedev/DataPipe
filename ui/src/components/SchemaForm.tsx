import { useState } from 'react'
import type { JsonSchema } from '../api/types'
import { useI18n } from '../i18n'

// A generic, schema-driven config form (UI-170 + CLAUDE.md: "Node config
// UIs are generated from JSON Schema in the node manifest — never
// hand-build a config form in the editor for a specific node type"). This
// component knows nothing about "inject", "set", or "debug-log" — it only
// ever looks at the JsonSchema it's given.

export interface SchemaFormProps {
  schema: JsonSchema
  value: unknown
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
}

export function SchemaForm({ schema, value, onChange, required, label }: SchemaFormProps) {
  if (schema.type === 'object' || (!schema.type && schema.properties)) {
    return <ObjectField schema={schema} value={asRecord(value)} onChange={onChange} label={label} />
  }
  if (schema.type === 'array') {
    return <ArrayField schema={schema} value={asArray(value)} onChange={onChange} required={required} label={label} />
  }
  if (schema.type === 'boolean') {
    return <BooleanField value={Boolean(value)} onChange={onChange} required={required} label={label} description={schema.description} />
  }
  if (schema.type === 'string' && schema.enum) {
    return (
      <EnumField
        options={schema.enum}
        value={typeof value === 'string' ? value : ''}
        onChange={onChange}
        required={required}
        label={label}
        description={schema.description}
      />
    )
  }
  if (schema.type === 'string') {
    return (
      <TextField
        value={typeof value === 'string' ? value : ''}
        onChange={onChange}
        required={required}
        label={label}
        description={schema.description}
      />
    )
  }
  if (schema.type === 'number' || schema.type === 'integer') {
    return (
      <NumberField
        value={typeof value === 'number' ? value : undefined}
        onChange={onChange}
        required={required}
        label={label}
        min={schema.minimum}
        description={schema.description}
      />
    )
  }
  // No declared type: an arbitrary JSON value (e.g. inject's "payload").
  return <AnyField value={value} onChange={onChange} required={required} label={label} description={schema.description} />
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {}
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function FieldLabel({ label, required, description }: { label?: string; required?: boolean; description?: string }) {
  const { t } = useI18n()
  if (!label) return null
  return (
    <div className="mb-1">
      <span className="text-sm font-medium">
        {label}
        {required && <span className="text-red-600"> *</span>}
      </span>
      {description && <p className="text-xs text-(--color-text-muted)">{description}</p>}
      {required && <span className="sr-only">{t('config.field.required')}</span>}
    </div>
  )
}

function ObjectField({
  schema,
  value,
  onChange,
  label,
}: {
  schema: JsonSchema
  value: Record<string, unknown>
  onChange: (value: unknown) => void
  label?: string
}) {
  const properties = schema.properties ?? {}
  const required = new Set(schema.required ?? [])

  function setField(key: string, fieldValue: unknown) {
    onChange({ ...value, [key]: fieldValue })
  }

  return (
    <div className={label ? 'rounded border border-(--color-border) p-3' : ''}>
      {label && <div className="mb-2 text-sm font-medium">{label}</div>}
      <div className="flex flex-col gap-3">
        {Object.entries(properties).map(([key, propSchema]) => (
          <SchemaForm
            key={key}
            schema={propSchema}
            value={value[key]}
            onChange={(v) => setField(key, v)}
            required={required.has(key)}
            label={key}
          />
        ))}
      </div>
    </div>
  )
}

function ArrayField({
  schema,
  value,
  onChange,
  required,
  label,
}: {
  schema: JsonSchema
  value: unknown[]
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
}) {
  const { t } = useI18n()
  const itemSchema: JsonSchema = schema.items ?? {}
  const showError = required && value.length === 0

  function updateItem(index: number, itemValue: unknown) {
    const next = [...value]
    next[index] = itemValue
    onChange(next)
  }
  function removeItem(index: number) {
    onChange(value.filter((_, i) => i !== index))
  }
  function addItem() {
    onChange([...value, defaultForSchema(itemSchema)])
  }

  return (
    <div>
      <FieldLabel label={label} required={required} description={schema.description} />
      <div className="flex flex-col gap-2">
        {value.map((item, index) => (
          <div key={index} className="flex items-start gap-2 rounded border border-(--color-border) p-2">
            <div className="flex-1">
              <SchemaForm schema={itemSchema} value={item} onChange={(v) => updateItem(index, v)} />
            </div>
            <button
              type="button"
              onClick={() => removeItem(index)}
              aria-label={t('common.delete')}
              className="rounded border border-(--color-border) px-2 py-1 text-xs"
            >
              ✕
            </button>
          </div>
        ))}
      </div>
      <button
        type="button"
        onClick={addItem}
        className="mt-2 rounded border border-(--color-border) px-2 py-1 text-xs"
      >
        +
      </button>
      {showError && <p className="mt-1 text-xs text-red-600">{t('config.field.required')}</p>}
    </div>
  )
}

function TextField({
  value,
  onChange,
  required,
  label,
  description,
}: {
  value: string
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
  description?: string
}) {
  const { t } = useI18n()
  const showError = required && value.trim() === ''
  return (
    <label className="block">
      <FieldLabel label={label} required={required} description={description} />
      <input
        className="w-full rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      {showError && <p className="mt-1 text-xs text-red-600">{t('config.field.required')}</p>}
    </label>
  )
}

function EnumField({
  options,
  value,
  onChange,
  required,
  label,
  description,
}: {
  options: string[]
  value: string
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
  description?: string
}) {
  return (
    <label className="block">
      <FieldLabel label={label} required={required} description={description} />
      <select
        className="w-full rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="" disabled>
          —
        </option>
        {options.map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
    </label>
  )
}

function NumberField({
  value,
  onChange,
  required,
  label,
  min,
  description,
}: {
  value: number | undefined
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
  min?: number
  description?: string
}) {
  const { t } = useI18n()
  const showError = required && value === undefined
  return (
    <label className="block">
      <FieldLabel label={label} required={required} description={description} />
      <input
        type="number"
        min={min}
        className="w-full rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value === '' ? undefined : Number(e.target.value))}
      />
      {showError && <p className="mt-1 text-xs text-red-600">{t('config.field.required')}</p>}
    </label>
  )
}

function BooleanField({
  value,
  onChange,
  required,
  label,
  description,
}: {
  value: boolean
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
  description?: string
}) {
  return (
    <label className="flex items-center gap-2">
      <input type="checkbox" checked={value} onChange={(e) => onChange(e.target.checked)} />
      <FieldLabel label={label} required={required} description={description} />
    </label>
  )
}

// AnyField backs untyped schema properties (e.g. inject's "payload"): any
// JSON value, with a literal/expression toggle (MAP-130 UI affordance — the
// engine's expression evaluation itself isn't implemented yet, see
// TODO.md).
function AnyField({
  value,
  onChange,
  required,
  label,
  description,
}: {
  value: unknown
  onChange: (value: unknown) => void
  required?: boolean
  label?: string
  description?: string
}) {
  const { t } = useI18n()
  const initialIsExpression = typeof value === 'string' && value.startsWith('={{') && value.endsWith('}}')
  const [mode, setMode] = useState<'literal' | 'expression'>(initialIsExpression ? 'expression' : 'literal')
  const [literalText, setLiteralText] = useState(() => (initialIsExpression ? '' : jsonStringifyOrEmpty(value)))
  const [exprText, setExprText] = useState(() => (initialIsExpression ? (value as string).slice(3, -2) : ''))
  const [parseError, setParseError] = useState(false)

  function commitLiteral(text: string) {
    setLiteralText(text)
    if (text.trim() === '') {
      setParseError(Boolean(required))
      onChange(undefined)
      return
    }
    try {
      onChange(JSON.parse(text))
      setParseError(false)
    } catch {
      // Not valid JSON: treat it as a plain string literal.
      onChange(text)
      setParseError(false)
    }
  }

  function commitExpression(text: string) {
    setExprText(text)
    onChange(`={{${text}}}`)
  }

  return (
    <div>
      <div className="flex items-center justify-between">
        <FieldLabel label={label} required={required} description={description} />
        <div className="flex text-xs">
          <button
            type="button"
            onClick={() => setMode('literal')}
            className={`rounded-l border border-(--color-border) px-2 py-0.5 ${mode === 'literal' ? 'bg-(--color-accent) text-white' : ''}`}
          >
            {t('config.field.literal')}
          </button>
          <button
            type="button"
            onClick={() => setMode('expression')}
            className={`rounded-r border border-(--color-border) px-2 py-0.5 ${mode === 'expression' ? 'bg-(--color-accent) text-white' : ''}`}
          >
            {t('config.field.expression')}
          </button>
        </div>
      </div>
      {mode === 'literal' ? (
        <input
          className="w-full rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
          value={literalText}
          onChange={(e) => commitLiteral(e.target.value)}
        />
      ) : (
        <div className="flex items-center rounded border border-(--color-border) px-2 py-1 text-sm">
          <span className="text-(--color-text-muted)">{'={{'}</span>
          <input
            className="flex-1 bg-transparent px-1"
            value={exprText}
            onChange={(e) => commitExpression(e.target.value)}
          />
          <span className="text-(--color-text-muted)">{'}}'}</span>
        </div>
      )}
      {parseError && <p className="mt-1 text-xs text-red-600">{t('config.field.required')}</p>}
    </div>
  )
}

function jsonStringifyOrEmpty(value: unknown): string {
  if (value === undefined) return ''
  try {
    return JSON.stringify(value)
  } catch {
    return ''
  }
}

function defaultForSchema(schema: JsonSchema): unknown {
  if (schema.default !== undefined) return schema.default
  switch (schema.type) {
    case 'object':
      return {}
    case 'array':
      return []
    case 'string':
      return ''
    case 'number':
    case 'integer':
      return 0
    case 'boolean':
      return false
    default:
      return undefined
  }
}
