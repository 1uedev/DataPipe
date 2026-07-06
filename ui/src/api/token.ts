// Shared, non-React token storage. Kept separate from the auth store so
// api/client.ts (which needs to read the token on every request) and
// store/auth.ts (which needs to write it on login/logout) don't have to
// import each other.
const STORAGE_KEY = 'datapipe.token'

let token: string | null = localStorage.getItem(STORAGE_KEY)

export function getToken(): string | null {
  return token
}

export function setToken(next: string | null): void {
  token = next
  if (next) {
    localStorage.setItem(STORAGE_KEY, next)
  } else {
    localStorage.removeItem(STORAGE_KEY)
  }
}
