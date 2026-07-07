import { useEffect, useState, type FormEvent } from 'react'
import * as api from '../api/resources'
import type { RuntimeEnrollToken, RuntimeGroup, RuntimeInfo } from '../api/types'
import { useAuthStore } from '../store/auth'
import { useI18n } from '../i18n'

// EDGE-120: fleet inventory (health/group/enrollment), runtime-group CRUD,
// and enrollment-token issuance (ARC-210 per-device credentials).
export default function Fleet() {
  const { t } = useI18n()
  const isSystemAdmin = useAuthStore((s) => s.user?.systemRole === 'system_admin')

  const [runtimes, setRuntimes] = useState<RuntimeInfo[] | null>(null)
  const [groups, setGroups] = useState<RuntimeGroup[] | null>(null)
  const [tokens, setTokens] = useState<RuntimeEnrollToken[] | null>(null)

  function reloadRuntimes() {
    void api.listRuntimes().then(setRuntimes)
  }
  function reloadGroups() {
    void api.listRuntimeGroups().then(setGroups)
  }
  function reloadTokens() {
    if (!isSystemAdmin) return
    void api.listEnrollTokens().then(setTokens)
  }

  useEffect(reloadRuntimes, [])
  useEffect(reloadGroups, [])
  useEffect(reloadTokens, [isSystemAdmin])

  return (
    <div className="mx-auto max-w-4xl p-6">
      <h1 className="mb-1 text-lg font-semibold">{t('fleet.title')}</h1>
      {!isSystemAdmin && <p className="mb-4 text-xs text-(--color-text-muted)">{t('fleet.systemAdminOnly')}</p>}

      <RuntimeTable
        runtimes={runtimes}
        groups={groups}
        isSystemAdmin={isSystemAdmin}
        onChanged={reloadRuntimes}
      />

      <div className="mt-8 grid gap-6 sm:grid-cols-2">
        <GroupsPanel groups={groups} isSystemAdmin={isSystemAdmin} onChanged={reloadGroups} />
        {isSystemAdmin && <TokensPanel tokens={tokens} groups={groups} onChanged={reloadTokens} />}
      </div>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MB`
  return `${(bytes / 1024).toFixed(0)} KB`
}

function RuntimeTable({
  runtimes,
  groups,
  isSystemAdmin,
  onChanged,
}: {
  runtimes: RuntimeInfo[] | null
  groups: RuntimeGroup[] | null
  isSystemAdmin: boolean
  onChanged: () => void
}) {
  const { t } = useI18n()

  if (runtimes === null) return <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
  if (runtimes.length === 0) return <p className="text-sm text-(--color-text-muted)">{t('fleet.empty')}</p>

  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-(--color-border) text-left text-xs text-(--color-text-muted)">
          <th className="py-2 pr-2">{t('fleet.runtimeId')}</th>
          <th className="py-2 pr-2">{t('fleet.kind')}</th>
          <th className="py-2 pr-2">{t('fleet.version')}</th>
          <th className="py-2 pr-2">{t('fleet.cpu')}</th>
          <th className="py-2 pr-2">{t('fleet.memory')}</th>
          <th className="py-2 pr-2">{t('fleet.flows')}</th>
          <th className="py-2 pr-2">{t('fleet.assignGroup')}</th>
          <th className="py-2 pr-2">{t('fleet.enrolled')}</th>
        </tr>
      </thead>
      <tbody>
        {runtimes.map((rt) => (
          <RuntimeRow key={rt.runtimeId} runtime={rt} groups={groups} isSystemAdmin={isSystemAdmin} onChanged={onChanged} />
        ))}
      </tbody>
    </table>
  )
}

function RuntimeRow({
  runtime,
  groups,
  isSystemAdmin,
  onChanged,
}: {
  runtime: RuntimeInfo
  groups: RuntimeGroup[] | null
  isSystemAdmin: boolean
  onChanged: () => void
}) {
  const { t } = useI18n()
  const [group, setGroup] = useState(runtime.group ?? '')
  const [saving, setSaving] = useState(false)

  async function onAssign() {
    setSaving(true)
    try {
      await api.updateRuntime(runtime.runtimeId, { group: group || null })
      onChanged()
    } finally {
      setSaving(false)
    }
  }

  return (
    <tr className="border-b border-(--color-border)">
      <td className="py-2 pr-2 font-mono text-xs">
        {runtime.displayName ? `${runtime.displayName} (${runtime.runtimeId})` : runtime.runtimeId}
      </td>
      <td className="py-2 pr-2">{runtime.kind}</td>
      <td className="py-2 pr-2">{runtime.version}</td>
      <td className="py-2 pr-2">
        <span className={runtime.online ? '' : 'text-(--color-text-muted)'}>
          {runtime.online ? (runtime.cpuPercent != null ? `${runtime.cpuPercent.toFixed(0)}%` : '—') : t('fleet.status.offline')}
        </span>
      </td>
      <td className="py-2 pr-2">{runtime.online && runtime.memoryBytes != null ? formatBytes(runtime.memoryBytes) : '—'}</td>
      <td className="py-2 pr-2">{runtime.flowCount}</td>
      <td className="py-2 pr-2">
        {isSystemAdmin ? (
          <div className="flex items-center gap-1">
            <select
              value={group}
              onChange={(e) => setGroup(e.target.value)}
              className="rounded border border-(--color-border) bg-transparent px-1 py-0.5 text-xs"
            >
              <option value="">{t('fleet.group.none')}</option>
              {(groups ?? []).map((g) => (
                <option key={g.name} value={g.name}>
                  {g.name}
                </option>
              ))}
            </select>
            {group !== (runtime.group ?? '') && (
              <button
                onClick={() => void onAssign()}
                disabled={saving}
                className="rounded border border-(--color-border) px-1.5 py-0.5 text-xs disabled:opacity-50"
              >
                {t('fleet.save')}
              </button>
            )}
          </div>
        ) : (
          (runtime.group ?? t('fleet.group.none'))
        )}
      </td>
      <td className="py-2 pr-2 text-xs">{runtime.enrolled ? t('fleet.enrolled') : t('fleet.notEnrolled')}</td>
    </tr>
  )
}

function GroupsPanel({
  groups,
  isSystemAdmin,
  onChanged,
}: {
  groups: RuntimeGroup[] | null
  isSystemAdmin: boolean
  onChanged: () => void
}) {
  const { t } = useI18n()
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [creating, setCreating] = useState(false)

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    setCreating(true)
    try {
      await api.createRuntimeGroup(name, description)
      setName('')
      setDescription('')
      onChanged()
    } finally {
      setCreating(false)
    }
  }

  async function onDelete(groupName: string) {
    await api.deleteRuntimeGroup(groupName)
    onChanged()
  }

  return (
    <section className="rounded border border-(--color-border) p-4">
      <h2 className="mb-3 text-sm font-semibold">{t('fleet.groups.title')}</h2>

      {groups === null ? (
        <p className="text-xs text-(--color-text-muted)">{t('common.loading')}</p>
      ) : groups.length === 0 ? (
        <p className="mb-3 text-xs text-(--color-text-muted)">{t('fleet.groups.empty')}</p>
      ) : (
        <ul className="mb-3 divide-y divide-(--color-border)">
          {groups.map((g) => (
            <li key={g.name} className="flex items-center justify-between py-1.5 text-sm">
              <div>
                <span className="font-medium">{g.name}</span>
                {g.description && <span className="ml-2 text-xs text-(--color-text-muted)">{g.description}</span>}
              </div>
              {isSystemAdmin && (
                <button onClick={() => void onDelete(g.name)} className="text-xs text-(--color-text-muted)">
                  {t('fleet.groups.delete')}
                </button>
              )}
            </li>
          ))}
        </ul>
      )}

      {isSystemAdmin && (
        <form onSubmit={onCreate} className="flex flex-col gap-2">
          <input
            className="rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
            placeholder={t('fleet.groups.name')}
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
          <input
            className="rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
            placeholder={t('fleet.groups.description')}
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
          <button
            type="submit"
            disabled={creating}
            className="self-start rounded bg-(--color-accent) px-3 py-1 text-sm font-medium text-white disabled:opacity-50"
          >
            {t('fleet.groups.create')}
          </button>
        </form>
      )}
    </section>
  )
}

function TokensPanel({
  tokens,
  groups,
  onChanged,
}: {
  tokens: RuntimeEnrollToken[] | null
  groups: RuntimeGroup[] | null
  onChanged: () => void
}) {
  const { t } = useI18n()
  const [displayName, setDisplayName] = useState('')
  const [group, setGroup] = useState('')
  const [issuing, setIssuing] = useState(false)
  const [justCreated, setJustCreated] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  async function onIssue(e: FormEvent) {
    e.preventDefault()
    setIssuing(true)
    setCopied(false)
    try {
      const created = await api.createEnrollToken(displayName || undefined, group || undefined)
      setJustCreated(created.token)
      setDisplayName('')
      setGroup('')
      onChanged()
    } finally {
      setIssuing(false)
    }
  }

  async function onRevoke(id: string) {
    await api.deleteEnrollToken(id)
    onChanged()
  }

  async function onCopy() {
    if (!justCreated) return
    await navigator.clipboard.writeText(justCreated)
    setCopied(true)
  }

  return (
    <section className="rounded border border-(--color-border) p-4">
      <h2 className="mb-3 text-sm font-semibold">{t('fleet.tokens.title')}</h2>

      {justCreated && (
        <div className="mb-3 rounded border border-(--color-accent) p-2 text-xs">
          <p className="mb-1 font-medium">{t('fleet.tokens.created.title')}</p>
          <div className="flex items-center gap-2">
            <code className="break-all">{justCreated}</code>
            <button onClick={() => void onCopy()} className="shrink-0 rounded border border-(--color-border) px-1.5 py-0.5">
              {copied ? t('fleet.tokens.copied') : t('fleet.tokens.copy')}
            </button>
          </div>
          <p className="mt-1 text-(--color-text-muted)">{t('fleet.tokens.envHint')}</p>
        </div>
      )}

      {tokens === null ? (
        <p className="text-xs text-(--color-text-muted)">{t('common.loading')}</p>
      ) : tokens.length === 0 ? (
        <p className="mb-3 text-xs text-(--color-text-muted)">{t('fleet.tokens.empty')}</p>
      ) : (
        <ul className="mb-3 divide-y divide-(--color-border)">
          {tokens.map((tok) => (
            <li key={tok.id} className="flex items-center justify-between py-1.5 text-xs">
              <div>
                <div className="font-medium">{tok.displayName || tok.id}</div>
                <div className="text-(--color-text-muted)">
                  {tok.group && `${tok.group} · `}
                  {tok.usedByRuntimeId ? t('fleet.tokens.usedBy', { runtimeId: tok.usedByRuntimeId }) : t('fleet.tokens.unused')}
                </div>
              </div>
              {tok.revoked ? (
                <span className="text-(--color-text-muted)">{t('fleet.tokens.revoked')}</span>
              ) : (
                <button onClick={() => void onRevoke(tok.id)} className="text-(--color-text-muted)">
                  {t('fleet.tokens.revoke')}
                </button>
              )}
            </li>
          ))}
        </ul>
      )}

      <form onSubmit={onIssue} className="flex flex-col gap-2">
        <input
          className="rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
          placeholder={t('fleet.tokens.displayName')}
          value={displayName}
          onChange={(e) => setDisplayName(e.target.value)}
        />
        <select
          value={group}
          onChange={(e) => setGroup(e.target.value)}
          className="rounded border border-(--color-border) bg-transparent px-2 py-1 text-sm"
        >
          <option value="">{t('fleet.tokens.group')}</option>
          {(groups ?? []).map((g) => (
            <option key={g.name} value={g.name}>
              {g.name}
            </option>
          ))}
        </select>
        <button
          type="submit"
          disabled={issuing}
          className="self-start rounded bg-(--color-accent) px-3 py-1 text-sm font-medium text-white disabled:opacity-50"
        >
          {t('fleet.tokens.issue')}
        </button>
      </form>
    </section>
  )
}
