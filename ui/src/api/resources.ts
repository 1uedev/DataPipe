import { request } from './client'
import type {
  AsyncAccepted,
  AuditEntry,
  Connection,
  ConnectionTestResult,
  CredentialMeta,
  DeadLetter,
  DebugPin,
  ExecuteNodeResult,
  Execution,
  ExecutionDetail,
  ExecutionStatus,
  Flow,
  FlowFileContent,
  FlowVersion,
  NodeType,
  PreviewResult,
  Project,
  RuntimeInfo,
  User,
} from './types'

// --- auth ---

export function login(username: string, password: string) {
  return request<{ token: string; expiresAt: string }>('/auth/login', {
    method: 'POST',
    body: { username, password },
  })
}

export function logout() {
  return request<void>('/auth/logout', { method: 'POST' })
}

export function me() {
  return request<User>('/auth/me')
}

// --- projects ---

export function listProjects() {
  return request<Project[]>('/projects')
}

export function createProject(name: string, description: string) {
  return request<Project>('/projects', { method: 'POST', body: { name, description } })
}

export function getProject(projectId: string) {
  return request<Project>(`/projects/${projectId}`)
}

// --- flows ---

export function listFlows(projectId: string) {
  return request<Flow[]>(`/projects/${projectId}/flows`)
}

export function createFlow(projectId: string, name: string, content: FlowFileContent) {
  return request<Flow>(`/projects/${projectId}/flows`, { method: 'POST', body: { name, content } })
}

export function getFlow(flowId: string) {
  return request<Flow>(`/flows/${flowId}`)
}

export function updateFlow(flowId: string, patch: { name?: string; content?: FlowFileContent }) {
  return request<Flow>(`/flows/${flowId}`, { method: 'PATCH', body: patch })
}

export function deleteFlow(flowId: string) {
  return request<void>(`/flows/${flowId}`, { method: 'DELETE' })
}

export function deployFlow(flowId: string, comment: string) {
  return request<FlowVersion>(`/flows/${flowId}/deploy`, { method: 'POST', body: { comment } })
}

export function listFlowVersions(flowId: string) {
  return request<FlowVersion[]>(`/flows/${flowId}/versions`)
}

export function rollbackFlow(flowId: string, version: number) {
  return request<FlowVersion>(`/flows/${flowId}/versions/${version}/rollback`, { method: 'POST' })
}

// --- connections & credentials ---

export function listConnections(projectId: string) {
  return request<Connection[]>(`/projects/${projectId}/connections`)
}

export function createConnection(
  projectId: string,
  name: string,
  type: string,
  config: Record<string, unknown>,
) {
  return request<Connection>(`/projects/${projectId}/connections`, {
    method: 'POST',
    body: { name, type, config },
  })
}

export function deleteConnection(connectionId: string) {
  return request<void>(`/connections/${connectionId}`, { method: 'DELETE' })
}

export function testConnection(connectionId: string) {
  return request<ConnectionTestResult>(`/connections/${connectionId}/test`, { method: 'POST' })
}

export function listCredentials(projectId: string) {
  return request<CredentialMeta[]>(`/projects/${projectId}/credentials`)
}

// --- runtimes, node types, audit log ---

export function listRuntimes() {
  return request<RuntimeInfo[]>('/runtimes')
}

export function listNodeTypes() {
  return request<NodeType[]>('/node-types')
}

export function listAuditLog(projectId?: string) {
  const suffix = projectId ? `?projectId=${encodeURIComponent(projectId)}` : ''
  return request<AuditEntry[]>(`/audit-log${suffix}`)
}

// --- Increment 5: live debugging (DBG-100/110/120/130/170) ---

export function executeNode(flowId: string, nodeId: string, payload: unknown) {
  return request<ExecuteNodeResult>(`/flows/${flowId}/nodes/${nodeId}/execute`, {
    method: 'POST',
    body: { payload },
  })
}

export function listPins(flowId: string) {
  return request<DebugPin[]>(`/flows/${flowId}/debug/pins`)
}

export function setPin(flowId: string, nodeId: string, port: string, value: unknown) {
  return request<DebugPin>(`/flows/${flowId}/nodes/${nodeId}/pins/${port}`, {
    method: 'PUT',
    body: { value },
  })
}

export function deletePin(flowId: string, nodeId: string, port: string) {
  return request<void>(`/flows/${flowId}/nodes/${nodeId}/pins/${port}`, { method: 'DELETE' })
}

export function loadFullDebugEvent(flowId: string, eventId: string) {
  return request<{ valueJson: string }>(`/flows/${flowId}/debug/events/${eventId}`)
}

// --- Increment 6: MAP-110 "fetch sample now" ---

export function previewNode(flowId: string, nodeId: string) {
  return request<PreviewResult>(`/flows/${flowId}/nodes/${nodeId}/preview`, { method: 'POST' })
}

// --- Increment 8: triggered workflows (ENG-130/DBG-140/ERR-130) ---

export function listExecutions(flowId: string, status?: ExecutionStatus, limit = 50, offset = 0) {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  if (status) params.set('status', status)
  return request<Execution[]>(`/flows/${flowId}/executions?${params.toString()}`)
}

export function getExecution(executionId: string) {
  return request<ExecutionDetail>(`/executions/${executionId}`)
}

export function rerunExecution(executionId: string, from: 'start' | 'node', nodeId?: string) {
  return request<AsyncAccepted>(`/executions/${executionId}/rerun`, {
    method: 'POST',
    body: { from, nodeId },
  })
}

export function cancelExecution(executionId: string) {
  return request<AsyncAccepted>(`/executions/${executionId}/cancel`, { method: 'POST' })
}

export function listDeadLetters(flowId: string, limit = 50, offset = 0) {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) })
  return request<DeadLetter[]>(`/flows/${flowId}/dead-letters?${params.toString()}`)
}

export function deleteDeadLetter(deadLetterId: string) {
  return request<void>(`/dead-letters/${deadLetterId}`, { method: 'DELETE' })
}

export function reinjectDeadLetter(deadLetterId: string) {
  return request<AsyncAccepted>(`/dead-letters/${deadLetterId}/reinject`, { method: 'POST' })
}
