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
