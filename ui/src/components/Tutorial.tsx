// UI-330's in-product interactive tutorial: builds a first flow
// (inject -> transform -> debug). Genuinely interactive — each step's
// "done" state is derived live from the canvas (nodes/edges), not just a
// static walkthrough the user clicks through blind.
import { useEditorStore } from '../store/editor'
import { useI18n } from '../i18n'

interface TutorialProps {
  deployed: boolean
  onClose: () => void
}

export function Tutorial({ deployed, onClose }: TutorialProps) {
  const { t } = useI18n()
  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)

  const injectNode = nodes.find((n) => n.data.nodeType === 'inject')
  const debugNode = nodes.find((n) => n.data.nodeType === 'debug-log')
  const step1 = injectNode != null
  const step2 = step1 && edges.some((e) => e.source === injectNode.id && e.target !== debugNode?.id)
  const transformNodeId = step2 ? edges.find((e) => e.source === injectNode!.id)?.target : undefined
  const step3 = debugNode != null && edges.some((e) => e.target === debugNode.id && e.source === transformNodeId)
  const step4 = deployed

  const steps = [
    { key: 'inject', label: t('tutorial.step1'), done: step1 },
    { key: 'transform', label: t('tutorial.step2'), done: step2 },
    { key: 'debug', label: t('tutorial.step3'), done: step3 },
    { key: 'deploy', label: t('tutorial.step4'), done: step4 },
  ]

  return (
    <div className="absolute top-2 right-2 z-10 w-72 rounded border border-(--color-border) bg-(--color-bg) p-3 shadow-lg">
      <div className="mb-2 flex items-center justify-between">
        <h2 className="text-sm font-semibold">{t('tutorial.title')}</h2>
        <button onClick={onClose} aria-label={t('tutorial.close')} className="text-xs text-(--color-text-muted)">
          ✕
        </button>
      </div>
      <ol className="flex flex-col gap-2 text-sm">
        {steps.map((s) => (
          <li key={s.key} className="flex items-start gap-2">
            <span className={s.done ? 'text-(--color-accent)' : 'text-(--color-text-muted)'}>{s.done ? '✓' : '○'}</span>
            <span className={s.done ? 'text-(--color-text-muted) line-through' : ''}>{s.label}</span>
          </li>
        ))}
      </ol>
    </div>
  )
}
