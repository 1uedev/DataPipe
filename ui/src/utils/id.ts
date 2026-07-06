// Client-side id generation for new nodes/wires/flows before they're ever
// sent to the server. Not cryptographically significant — just needs to be
// unique within one flow document.
export function newId(prefix: string): string {
  const random = Math.random().toString(36).slice(2, 10)
  return `${prefix}_${random}`
}
