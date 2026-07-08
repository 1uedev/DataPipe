import { request } from './client'
import type {
  Alert,
  AlertRule,
  AsyncAccepted,
  AuditEntry,
  Connection,
  ConnectionTestResult,
  CredentialMeta,
  DeadLetter,
  DebugPin,
  EnvironmentProfile,
  ExecuteNodeResult,
  Execution,
  ExecutionDetail,
  ExecutionStatus,
  Flow,
  FlowExportBundle,
  FlowFileContent,
  FlowVersion,
  ImportResult,
  NodeType,
  PreviewResult,
  Project,
  RuntimeEnrollToken,
  RuntimeEnrollTokenCreated,
  RuntimeGroup,
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

export function deployFlow(flowId: string, comment: string, profileId?: string) {
  return request<FlowVersion>(`/flows/${flowId}/deploy`, { method: 'POST', body: { comment, profileId } })
}

export function setFlowLogLevel(flowId: string, level: 'debug' | 'info' | 'warn' | 'error') {
  return request<Flow>(`/flows/${flowId}/log-level`, { method: 'PATCH', body: { level } })
}

// --- VCS-140: environment profiles ---

export function listEnvProfiles(projectId: string) {
  return request<EnvironmentProfile[]>(`/projects/${projectId}/profiles`)
}

export function createEnvProfile(projectId: string, name: string, values: Record<string, string>) {
  return request<EnvironmentProfile>(`/projects/${projectId}/profiles`, { method: 'POST', body: { name, values } })
}

export function updateEnvProfile(profileId: string, values: Record<string, string>) {
  return request<EnvironmentProfile>(`/profiles/${profileId}`, { method: 'PATCH', body: { values } })
}

export function deleteEnvProfile(profileId: string) {
  return request<void>(`/profiles/${profileId}`, { method: 'DELETE' })
}

// --- VCS-130: import/export ---

export function exportFlow(flowId: string) {
  return request<FlowExportBundle>(`/flows/${flowId}/export`)
}

export function exportProject(projectId: string) {
  return request<FlowExportBundle>(`/projects/${projectId}/export`)
}

export function importProject(projectId: string, bundle: FlowExportBundle) {
  return request<ImportResult>(`/projects/${projectId}/import`, { method: 'POST', body: bundle })
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

// --- OBS-140: alerting hooks ---

export function listAlertRules() {
  return request<AlertRule[]>('/alert-rules')
}

export function createAlertRule(input: { name: string; metric: 'connectionDown' | 'edgeOffline'; targetRuntimeId?: string; webhookUrl?: string }) {
  return request<AlertRule>('/alert-rules', { method: 'POST', body: input })
}

export function deleteAlertRule(ruleId: string) {
  return request<void>(`/alert-rules/${ruleId}`, { method: 'DELETE' })
}

export function listAlerts() {
  return request<Alert[]>('/alerts')
}

// --- Increment 9: fleet management (EDGE-120/UI-220) ---

export function updateRuntime(runtimeId: string, patch: { displayName?: string; group?: string | null }) {
  return request<RuntimeInfo>(`/runtimes/${runtimeId}`, { method: 'PATCH', body: patch })
}

export function listRuntimeGroups() {
  return request<RuntimeGroup[]>('/runtime-groups')
}

export function createRuntimeGroup(name: string, description?: string) {
  return request<RuntimeGroup>('/runtime-groups', { method: 'POST', body: { name, description } })
}

export function deleteRuntimeGroup(name: string) {
  return request<void>(`/runtime-groups/${encodeURIComponent(name)}`, { method: 'DELETE' })
}

export function listEnrollTokens() {
  return request<RuntimeEnrollToken[]>('/runtime-enroll-tokens')
}

export function createEnrollToken(displayName?: string, group?: string) {
  return request<RuntimeEnrollTokenCreated>('/runtime-enroll-tokens', {
    method: 'POST',
    body: { displayName, group },
  })
}

export function deleteEnrollToken(tokenId: string) {
  return request<void>(`/runtime-enroll-tokens/${tokenId}`, { method: 'DELETE' })
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
