// Triggers a browser download of data as a JSON file. Used by VCS-130's
// flow/project export buttons — the bundle is fetched via the normal
// (bearer-token-authenticated) API client, so it can't just be a plain
// link to the endpoint URL; this saves the already-fetched JSON locally.
export function downloadJSON(filename: string, data: unknown): void {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}
