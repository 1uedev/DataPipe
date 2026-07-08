import { Link, Outlet } from 'react-router-dom'
import { useThemeStore } from '../store/theme'
import { useAuthStore } from '../store/auth'
import { useI18n } from '../i18n'

export default function Layout() {
  const theme = useThemeStore((s) => s.theme)
  const toggleTheme = useThemeStore((s) => s.toggle)
  const { t, locale, setLocale } = useI18n()
  const user = useAuthStore((s) => s.user)
  const logout = useAuthStore((s) => s.logout)

  return (
    <div className="flex min-h-screen flex-col">
      <header className="flex items-center justify-between border-b border-(--color-border) px-4 py-2">
        <div className="flex items-center gap-4">
          <Link to="/projects" className="text-sm font-semibold">
            {t('app.title')}
          </Link>
          <Link to="/fleet" className="text-sm text-(--color-text-muted) hover:text-(--color-text)">
            {t('fleet.title')}
          </Link>
          <Link to="/monitoring" className="text-sm text-(--color-text-muted) hover:text-(--color-text)">
            {t('monitoring.title')}
          </Link>
        </div>
        <div className="flex items-center gap-3 text-sm">
          <select
            aria-label={t('app.language')}
            value={locale}
            onChange={(e) => setLocale(e.target.value as 'en' | 'de')}
            className="rounded border border-(--color-border) bg-transparent px-1 py-0.5"
          >
            <option value="en">EN</option>
            <option value="de">DE</option>
          </select>
          <button
            onClick={toggleTheme}
            aria-label={theme === 'dark' ? t('app.theme.toggleToLight') : t('app.theme.toggleToDark')}
            className="rounded border border-(--color-border) px-2 py-0.5"
          >
            {theme === 'dark' ? '☀' : '☾'}
          </button>
          {user && (
            <>
              <span className="text-(--color-text-muted)">{user.username}</span>
              <button onClick={() => void logout()} className="rounded border border-(--color-border) px-2 py-0.5">
                {t('auth.logout')}
              </button>
            </>
          )}
        </div>
      </header>
      <main className="flex-1">
        <Outlet />
      </main>
    </div>
  )
}
