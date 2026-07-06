import { useEffect, useState, type FormEvent } from 'react'
import { Link, useParams } from 'react-router-dom'
import * as api from '../api/resources'
import type { Flow, Project } from '../api/types'
import { useI18n } from '../i18n'
import { emptyFlowContent } from '../utils/flowFile'

export default function ProjectDetail() {
  const { t } = useI18n()
  const { projectId } = useParams<{ projectId: string }>()
  const [project, setProject] = useState<Project | null>(null)
  const [flows, setFlows] = useState<Flow[] | null>(null)
  const [name, setName] = useState('')
  const [creating, setCreating] = useState(false)

  useEffect(() => {
    if (!projectId) return
    void api.getProject(projectId).then(setProject)
    void api.listFlows(projectId).then((data) => setFlows(data ?? []))
  }, [projectId])

  async function onCreate(e: FormEvent) {
    e.preventDefault()
    if (!projectId) return
    setCreating(true)
    try {
      const flow = await api.createFlow(projectId, name, emptyFlowContent(name))
      setFlows((prev) => [...(prev ?? []), flow])
      setName('')
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="mx-auto max-w-2xl p-6">
      <Link to="/projects" className="text-sm text-(--color-accent)">
        ← {t('flows.backToProjects')}
      </Link>
      <h1 className="mt-2 mb-4 text-xl font-semibold">{project?.name ?? t('common.loading')}</h1>

      <h2 className="mb-2 text-sm font-semibold text-(--color-text-muted)">{t('flows.title')}</h2>
      {flows === null ? (
        <p className="text-sm text-(--color-text-muted)">{t('common.loading')}</p>
      ) : flows.length === 0 ? (
        <p className="text-sm text-(--color-text-muted)">{t('flows.empty')}</p>
      ) : (
        <ul className="mb-6 divide-y divide-(--color-border) rounded border border-(--color-border)">
          {flows.map((f) => (
            <li key={f.id} className="flex items-center justify-between px-3 py-2">
              <div>
                <div className="text-sm font-medium">{f.name}</div>
                <div className="text-xs text-(--color-text-muted)">
                  {f.deployedVersion != null
                    ? t('flows.deployedVersion', { version: f.deployedVersion })
                    : t('flows.notDeployed')}
                </div>
              </div>
              <Link
                to={`/projects/${projectId}/flows/${f.id}`}
                className="rounded border border-(--color-border) px-2 py-1 text-xs"
              >
                {t('flows.open')}
              </Link>
            </li>
          ))}
        </ul>
      )}

      <form onSubmit={onCreate} className="rounded border border-(--color-border) p-4">
        <h2 className="mb-3 text-sm font-semibold">{t('flows.create')}</h2>
        <label className="mb-3 block text-sm">
          {t('flows.name')}
          <input
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </label>
        <button
          type="submit"
          disabled={creating}
          className="rounded bg-(--color-accent) px-3 py-1.5 text-sm font-medium text-white disabled:opacity-50"
        >
          {t('flows.create.submit')}
        </button>
      </form>
    </div>
  )
}
