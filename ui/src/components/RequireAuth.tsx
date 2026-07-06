import { useEffect } from 'react'
import { Navigate, Outlet } from 'react-router-dom'
import { useAuthStore } from '../store/auth'
import { useI18n } from '../i18n'

export default function RequireAuth() {
  const { t } = useI18n()
  const status = useAuthStore((s) => s.status)
  const checkSession = useAuthStore((s) => s.checkSession)

  useEffect(() => {
    if (status === 'checking') void checkSession()
  }, [status, checkSession])

  if (status === 'checking') {
    return <div className="p-6 text-sm text-(--color-text-muted)">{t('common.loading')}</div>
  }
  if (status === 'unauthenticated') {
    return <Navigate to="/login" replace />
  }
  return <Outlet />
}
