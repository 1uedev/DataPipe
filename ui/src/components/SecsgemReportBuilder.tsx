import { useState } from 'react'
import * as api from '../api/resources'
import type { SecsgemSVID } from '../api/types'
import { useI18n } from '../i18n'

// MAP-100 SECS/GEM report builder: browses a "secsgem" connection's live
// SVID catalog (S1F11) so the user can pick real SVIDs instead of typing
// identifiers from memory. Shown as a supplementary block alongside the
// "secsgem-in" node's schema-driven config form (mirrors the MAP-110
// "fetch sample now" pattern: a live-data helper next to, not replacing,
// the generated form) — CEIDs have no equivalent equipment-side discovery
// message in the base GEM standard, so they still come from the
// equipment's own SECS/GEM manual, typed directly into the config.
interface SecsgemReportBuilderProps {
  connectionId?: string
}

export function SecsgemReportBuilder({ connectionId }: SecsgemReportBuilderProps) {
  const { t } = useI18n()
  const [svids, setSvids] = useState<SecsgemSVID[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [copied, setCopied] = useState<number | null>(null)

  async function onBrowse() {
    if (!connectionId) return
    setLoading(true)
    setError(null)
    try {
      const result = await api.secsgemBrowse(connectionId)
      if (result.ok) {
        setSvids(result.svids ?? [])
      } else {
        setError(result.message ?? 'unknown error')
        setSvids(null)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      setSvids(null)
    } finally {
      setLoading(false)
    }
  }

  async function onCopy(svid: number) {
    try {
      await navigator.clipboard.writeText(String(svid))
      setCopied(svid)
      setTimeout(() => setCopied(null), 1500)
    } catch {
      // Clipboard access can be denied by the browser; the SVID is still
      // visible on screen for the user to type manually.
    }
  }

  return (
    <section className="mt-3 border-t border-(--color-border) pt-3 text-xs">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold">{t('secsgem.browser.title')}</h3>
        <button
          onClick={() => void onBrowse()}
          disabled={!connectionId || loading}
          className="rounded border border-(--color-border) px-2 py-1 disabled:opacity-50"
        >
          {loading ? t('secsgem.browser.running') : t('secsgem.browser.button')}
        </button>
      </div>
      {!connectionId && <p className="mt-2 text-(--color-text-muted)">{t('secsgem.browser.noConnection')}</p>}
      {error && <p className="mt-2 text-red-600">{error}</p>}
      {svids && (
        <div className="mt-2 flex flex-col gap-1">
          <p className="text-(--color-text-muted)">{t('secsgem.browser.hint')}</p>
          {svids.length === 0 && <p className="text-(--color-text-muted)">{t('secsgem.browser.empty')}</p>}
          {svids.map((sv) => (
            <div key={sv.svid} className="flex items-center justify-between rounded border border-(--color-border) p-1.5">
              <span>
                <span className="font-mono font-medium">{sv.svid}</span> — {sv.name}
                {sv.units && <span className="text-(--color-text-muted)"> ({sv.units})</span>}
              </span>
              <button onClick={() => void onCopy(sv.svid)} className="text-(--color-accent)">
                {copied === sv.svid ? '✓' : t('secsgem.browser.copy')}
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}
