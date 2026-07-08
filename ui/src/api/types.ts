// Mirrors docs/api/openapi.yaml's response schemas exactly — never invent
// fields here; if the API changes, the contract changes first.

export interface User {
  id: string
  username: string
  systemRole: 'none' | 'system_admin'
  createdAt: string
}

export interface Project {
  id: string
  name: string
  description: string
  createdAt: string
}

export interface Flow {
  id: string
  projectId: string
  name: string
  content: FlowFileContent
  deployedVersion: number | null
  logLevel: 'debug' | 'info' | 'warn' | 'error'
  createdAt: string
  updatedAt: string
}

export interface FlowVersion {
  flowId: string
  version: number
  content: FlowFileContent
  author: string
  comment: string
  createdAt: string
  deployedAt: string | null
}

export interface Connection {
  id: string
  projectId: string
  name: string
  type: string
  config: Record<string, unknown>
  credentialId: string | null
}

// CON-140 "test connection" result.
export interface ConnectionTestResult {
  ok: boolean
  message: string
}

// MAP-110 "fetch sample now" result.
export interface PreviewResult {
  records: { port: string; datagram: unknown }[]
  error: string | null
}

export interface CredentialMeta {
  id: string
  projectId: string
  name: string
  createdAt: string
}

export interface RuntimeInfo {
  runtimeId: string
  kind: 'server' | 'edge'
  version: string
  lastSeen: string
  online: boolean
  cpuPercent: number | null
  memoryBytes: number | null
  flowCount: number
  displayName: string | null
  group: string | null
  enrolled: boolean
}

export interface AlertRule {
  id: string
  name: string
  metric: 'connectionDown' | 'edgeOffline'
  targetRuntimeId?: string
  webhookUrl?: string
  enabled: boolean
  createdAt: string
}

export interface Alert {
  id: string
  ruleId: string
  ruleName: string
  state: 'firing' | 'resolved'
  message: string
  firedAt: string
  resolvedAt?: string
}

// --- Increment 9: fleet management (EDGE-120) ---

export interface RuntimeGroup {
  name: string
  description: string
  createdAt: string
}

export interface RuntimeEnrollToken {
  id: string
  displayName: string
  group: string
  createdAt: string
  usedByRuntimeId: string
  revoked: boolean
}

export interface RuntimeEnrollTokenCreated extends RuntimeEnrollToken {
  token: string
}

export interface AuditEntry {
  id: string
  at: string
  actorUserId: string
  action: string
  objectType: string
  objectId: string
  hash: string
}

export interface NodeType {
  type: string
  displayName: string
  category: 'source' | 'processor' | 'sink' | 'control'
  description: string
  kind: 'source' | 'processor'
  inputs: string[]
  outputs: string[]
  configSchema: JsonSchema
}

// A pragmatic subset of JSON Schema (draft 2020-12) — just what
// engine/flow node manifests actually use.
export interface JsonSchema {
  type?: 'object' | 'array' | 'string' | 'number' | 'integer' | 'boolean'
  properties?: Record<string, JsonSchema>
  items?: JsonSchema
  required?: string[]
  description?: string
  default?: unknown
  minimum?: number
  enum?: string[]
}

// --- Flow-File-Format.md §2 (the subset the editor needs) ---

export interface FlowFileContent {
  formatVersion: number
  kind: 'flow' | 'subflow'
  id: string
  name: string
  description?: string
  mode: 'streaming' | 'triggered'
  disabled?: boolean
  runtimeAssignment?: { group?: string } | null
  graph: FlowGraph
  layout?: FlowLayout
}

export interface FlowGraph {
  nodes: FlowNode[]
  wires: FlowWire[]
}

export interface FlowNode {
  id: string
  type: string
  typeVersion: number
  name?: string
  disabled?: boolean
  connection?: string
  config?: Record<string, unknown>
  errorPolicy?: { onError?: string }
  overflow?: string
}

export interface FlowWire {
  id: string
  from: { node: string; port: string }
  to: { node: string; port: string }
}

export interface FlowLayout {
  nodes?: Record<string, { x: number; y: number }>
}

// --- Increment 5: live debugging (DBG-100/110/120/130/170) ---

export type DebugDirection = 'in' | 'out' | 'sidebar'

// Mirrors docs/api/debug-websocket.md's "event" message.
export interface DebugEvent {
  id: string
  flowId: string
  nodeId: string
  port: string
  direction: DebugDirection
  label: string
  timeUnixMs: number
  datagramId: string
  correlationId: string
  causationId: string
  quality: string
  valueJson: string
  truncated: boolean
  fullLength: number
}

// Mirrors docs/api/debug-websocket.md's "wireMetrics" message.
export interface WireMetrics {
  flowId: string
  fromNode: string
  fromPort: string
  toNode: string
  toPort: string
  delivered: number
  dropped: number
}

export type DebugWSMessage =
  | { type: 'event'; event: DebugEvent }
  | { type: 'wireMetrics'; metrics: WireMetrics }

export interface ExecuteNodeResult {
  outputs: { port: string; datagram: unknown }[]
  error: string | null
}

export interface DebugPin {
  flowId: string
  nodeId: string
  port: string
  value: unknown
  updatedAt: string
}

// --- Increment 8: triggered workflows (ENG-130/DBG-140/ERR-130) ---

export type ExecutionStatus = 'running' | 'waiting' | 'success' | 'failed' | 'cancelled' | 'crashed'

export interface Execution {
  id: string
  flowId: string
  runtimeId: string
  status: ExecutionStatus
  triggerNodeId: string
  triggerKind: string
  reRunOf: string | null
  startedAt: string
  finishedAt: string | null
  durationMs: number | null
  reason: string
}

export interface ExecutionNodeIOError {
  message: string
  code: string
  stack: string
}

export interface ExecutionNodeIO {
  nodeId: string
  port: string
  attempt: number
  at: string
  durationUs: number
  input: unknown
  outputs: { port: string; datagram: unknown }[]
  error: ExecutionNodeIOError | null
}

export interface ExecutionDetail extends Execution {
  nodeIO: ExecutionNodeIO[]
}

export interface DeadLetter {
  id: string
  flowId: string
  nodeId: string
  port: string
  reason: string
  datagram: unknown
  createdAt: string
  reinjectedAt: string | null
}

export interface AsyncAccepted {
  accepted: boolean
}
