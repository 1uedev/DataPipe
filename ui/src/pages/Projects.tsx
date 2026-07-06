import { useEffect, useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import * as api from '../api/resources'
import type { Project } from '../api/types'
import { useI18n } from '../i18n'

export default function Projects() {
  const { t } = useI18n()
  const [projects, setProjects] = useState<Project[] | null>(null)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [creating, setCreating] = useState(false)

  useEffect(() => {
    void api.listProjects().then((data) => setProjects(data ?? []))
  }, [])

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    setCreating(true)
    try {
      const project = await api.createProject(name, description)
      setProjects((prev) => [...(prev ?? []), project])
      setName('')
      setDescription('')
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <h1 className="mb-4 text-xl font-semibold">{t('projects.title')}</h1>

      {projects === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : projects.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('projects.empty')}</p>
      ) : (
        <ul className="mb-6 divide-y divide-(--color-border) rounded border border-(--color-border)">
          {projects.map((p) => (
            <li key={p.id}>
              <Link to={`/projects/${p.id}`} className="block px-3 py-2 hover:bg-(--color-bg-subtle)">
                <div className="text-sm font-medium">{p.name}</div>
                {p.description && <div className="text-xs text-(--color-text-muted)">{p.description}</div>}
              </Link>
            </li>
          ))}
        </ul>
      )}

      <form onSubmit={onCreate} className="rounded border border-(--color-border) p-4">
        <h2 className="mb-3 text-sm font-semibold">{t('projects.create')}</h2>
        <label className="mb-2 block text-sm">
          {t('projects.name')}
          <input
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </label>
        <label className="mb-3 block text-sm">
          {t('projects.description')}
          <input
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
        </label>
        <button
          type="submit"
          disabled={creating}
          className="rounded bg-(--color-accent) px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          {t('projects.create.submit')}
        </button>
      </form>
    </div>
  )
}
