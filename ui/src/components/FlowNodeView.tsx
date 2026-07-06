import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { CanvasNode } from '../utils/flowConversion'
import type { NodeType } from '../api/types'

// UI-120: "icon + color by category (sources green, processors blue, sinks
// orange, control violet), user-assignable name, status dot
// (running/error/disabled) with message". Live datagrams/sec + last-value
// indicators need real data from the runtime (DBG-170, Increment 5) — not
// wired up yet, see TODO.md.
const CATEGORY_COLOR: Record<string, string> = {
  source: 'var(--color-source)',
  processor: 'var(--color-processor)',
  sink: 'var(--color-sink)',
  control: 'var(--color-control)',
}

export function makeFlowNodeView(nodeTypesByName: Map<string, NodeType>) {
  return function FlowNodeView({ data, selected }: NodeProps<CanvasNode>) {
    const nodeType = nodeTypesByName.get(data.nodeType)
    const color = CATEGORY_COLOR[nodeType?.category ?? ''] ?? 'var(--color-text-muted)'
    const inputs = nodeType?.inputs ?? []
    const outputs = nodeType?.outputs ?? []
    const hasErrorPort = data.errorPolicy?.onError === 'errorPort'

    return (
      <div
        className={`min-w-36 rounded-md border bg-(--color-bg) px-3 py-2 text-sm shadow-sm ${
          selected ? 'border-(--color-accent)' : 'border-(--color-border)'
        } ${data.disabled ? 'opacity-50' : ''}`}
        style={{ borderLeftWidth: 4, borderLeftColor: color }}
      >
        {inputs.map((port, i) => (
          <Handle key={port} id={port} type="target" position={Position.Left} style={{ top: 16 + i * 12 }} />
        ))}

        <div className="flex items-center gap-1.5">
          <span
            className="inline-block h-2 w-2 rounded-full"
            style={{ background: data.disabled ? 'var(--color-text-muted)' : color }}
            title={data.disabled ? 'disabled' : 'idle'}
          />
          <span className="truncate font-medium">{data.name || nodeType?.displayName || data.nodeType}</span>
        </div>
        <div className="text-xs text-(--color-text-muted)">{nodeType?.displayName ?? data.nodeType}</div>

        {outputs.map((port, i) => (
          <Handle key={port} id={port} type="source" position={Position.Right} style={{ top: 16 + i * 12 }} />
        ))}
        {hasErrorPort && (
          <Handle id="error" type="source" position={Position.Bottom} style={{ background: '#dc2626' }} />
        )}
      </div>
    )
  }
}
