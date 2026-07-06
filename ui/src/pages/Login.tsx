import { useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import { useI18n } from '../i18n'

export default function Login() {
  const { t } = useI18n()
  const login = useAuthStore((s) => s.login)
  const error = useAuthStore((s) => s.error)
  const navigate = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [submitting, setSubmitting] = useState(false)

  async function onSubmit(e: FormEvent) {
    e.preventDefault()
    setSubmitting(true)
    try {
      await login(username, password)
      navigate('/projects', { replace: true })
    } catch {
      // error is surfaced via the store
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-(--color-bg-subtle)">
      <form
        onSubmit={onSubmit}
        className="w-80 rounded-lg border border-(--color-border) bg-(--color-bg) p-6 shadow-sm"
      >
        <h1 className="mb-4 text-lg font-semibold">{t('auth.login.title')}</h1>

        <label className="mb-3 block text-sm">
          {t('auth.login.username')}
          <input
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus
            required
          />
        </label>

        <label className="mb-4 block text-sm">
          {t('auth.login.password')}
          <input
            type="password"
            className="mt-1 w-full rounded border border-(--color-border) bg-transparent px-2 py-1"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </label>

        {error && <p className="mb-3 text-sm text-red-600">{t('auth.login.error')}</p>}

        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded bg-(--color-accent) px-3 py-2 text-sm font-medium text-white disabled:opacity-50"
        >
          {t('auth.login.submit')}
        </button>
      </form>
    </div>
  )
}
