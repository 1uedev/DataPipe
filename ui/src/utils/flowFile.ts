import type { FlowFileContent } from '../api/types'
import { newId } from './id'

// A brand-new, empty streaming flow — Flow-File-Format.md §2's required
// fields with no nodes yet.
export function emptyFlowContent(name: string): FlowFileContent {
  return {
    formatVersion: 1,
    kind: 'flow',
    id: newId('flow'),
    name,
    mode: 'streaming',
    graph: { nodes: [], wires: [] },
    layout: { nodes: {} },
  }
}
