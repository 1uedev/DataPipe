import { describe, expect, it, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { I18nProvider, useI18n } from './index'

function Probe() {
  const { t, locale, setLocale } = useI18n()
  return (
    <div>
      <span data-testid="locale">{locale}</span>
      <span data-testid="greeting">{t('auth.login.title')}</span>
      <span data-testid="interpolated">{t('flows.deployedVersion', { version: 3 })}</span>
      <button onClick={() => setLocale('de')}>de</button>
    </div>
  )
}

describe('i18n', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('defaults to English and interpolates variables', () => {
    render(
      <I18nProvider>
        <Probe />
      </I18nProvider>,
    )
    expect(screen.getByTestId('locale')).toHaveTextContent('en')
    expect(screen.getByTestId('greeting')).toHaveTextContent('Sign in')
    expect(screen.getByTestId('interpolated')).toHaveTextContent('Deployed: v3')
  })

  it('switches locale and persists it', () => {
    render(
      <I18nProvider>
        <Probe />
      </I18nProvider>,
    )
    fireEvent.click(screen.getByText('de'))
    expect(screen.getByTestId('locale')).toHaveTextContent('de')
    expect(screen.getByTestId('greeting')).toHaveTextContent('Anmelden')
    expect(localStorage.getItem('datapipe.locale')).toBe('de')
  })
})
