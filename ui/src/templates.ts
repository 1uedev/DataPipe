// UI-330's template gallery: example flows importable per connector,
// reusing VCS-130's FlowExportBundle import mechanism verbatim (no new
// backend endpoint needed) — each template is a bundle with node
// `connection` fields already rewritten to bundle-local refs, exactly like
// a real export, so api.importProject remaps/creates connections the same
// way it would for a bundle downloaded from another project.
import type { FlowExportBundle } from './api/types'

export interface FlowTemplate {
  id: string
  name: string
  description: string
  bundle: FlowExportBundle
}

function flow(id: string, name: string, graph: FlowExportBundle['flows'][number]['content']['graph']): FlowExportBundle['flows'][number] {
  return {
    name,
    content: { formatVersion: 1, kind: 'flow', id, name, mode: 'streaming', graph },
  }
}

export const templates: FlowTemplate[] = [
  {
    id: 'inject-transform-debug',
    name: 'Inject → Transform → Debug',
    description: 'The tutorial flow itself: a timer emits a value, a Calculator node derives a second value, and a Debug node shows both live.',
    bundle: {
      formatVersion: 1,
      exportedAt: new Date(0).toISOString(),
      flows: [
        flow('flow_tpl_inject', 'Inject → Transform → Debug', {
          nodes: [
            { id: 'n1', type: 'inject', typeVersion: 1, config: { payload: { celsius: 20 }, repeatMs: 2000 } },
            {
              id: 'n2',
              type: 'calculator',
              typeVersion: 1,
              config: { fields: [{ path: 'fahrenheit', expression: 'payload.celsius * 9 / 5 + 32' }] },
            },
            { id: 'n3', type: 'debug-log', typeVersion: 1, config: {} },
          ],
          wires: [
            { id: 'w1', from: { node: 'n1', port: 'out' }, to: { node: 'n2', port: 'in' } },
            { id: 'w2', from: { node: 'n2', port: 'out' }, to: { node: 'n3', port: 'in' } },
          ],
        }),
      ],
      connections: [],
    },
  },
  {
    id: 'mqtt-to-debug',
    name: 'MQTT → Debug',
    description: 'Subscribes to an MQTT topic and shows every message live — a starting point for any MQTT-based flow (CON-200).',
    bundle: {
      formatVersion: 1,
      exportedAt: new Date(0).toISOString(),
      flows: [
        flow('flow_tpl_mqtt', 'MQTT → Debug', {
          nodes: [
            { id: 'n1', type: 'mqtt-in', typeVersion: 1, connection: 'ref:0', config: { topic: 'sensors/+/temperature', qos: 0 } },
            { id: 'n2', type: 'debug-log', typeVersion: 1, config: {} },
          ],
          wires: [{ id: 'w1', from: { node: 'n1', port: 'out' }, to: { node: 'n2', port: 'in' } }],
        }),
      ],
      connections: [{ ref: 'ref:0', name: 'mqtt-broker', type: 'mqtt', config: { host: 'localhost', port: 1883 }, hasCredential: false }],
    },
  },
  {
    id: 'schedule-http-debug',
    name: 'Schedule → HTTP Request → Debug',
    description: 'Polls a REST endpoint on a timer and shows the response — a starting point for any polling integration (CON-330/CON-315).',
    bundle: {
      formatVersion: 1,
      exportedAt: new Date(0).toISOString(),
      flows: [
        flow('flow_tpl_schedule_http', 'Schedule → HTTP Request → Debug', {
          nodes: [
            { id: 'n1', type: 'schedule', typeVersion: 1, config: { mode: 'interval', intervalMs: 60000 } },
            { id: 'n2', type: 'http-request', typeVersion: 1, config: { url: 'https://example.com/api/status', method: 'GET' } },
            { id: 'n3', type: 'debug-log', typeVersion: 1, config: {} },
          ],
          wires: [
            { id: 'w1', from: { node: 'n1', port: 'out' }, to: { node: 'n2', port: 'in' } },
            { id: 'w2', from: { node: 'n2', port: 'out' }, to: { node: 'n3', port: 'in' } },
          ],
        }),
      ],
      connections: [],
    },
  },
  {
    id: 'filewatch-sql-sink',
    name: 'File Watch → SQL Sink',
    description: 'Watches a directory for CSV files and inserts every row into a database table — a starting point for batch file ingestion (CON-400/SNK-190).',
    bundle: {
      formatVersion: 1,
      exportedAt: new Date(0).toISOString(),
      flows: [
        flow('flow_tpl_filewatch_sql', 'File Watch → SQL Sink', {
          nodes: [
            {
              id: 'n1',
              type: 'file-watch',
              typeVersion: 1,
              config: { directory: '/data/incoming', pattern: '*.csv', format: 'csv', csv: { hasHeader: true } },
            },
            {
              id: 'n2',
              type: 'sql-sink',
              typeVersion: 1,
              connection: 'ref:0',
              config: { mode: 'insert', table: 'readings', columns: ['sensor_id', 'value', 'timestamp'] },
            },
          ],
          wires: [{ id: 'w1', from: { node: 'n1', port: 'out' }, to: { node: 'n2', port: 'in' } }],
        }),
      ],
      connections: [{ ref: 'ref:0', name: 'postgres-main', type: 'postgres', config: { host: 'localhost', port: 5432, database: 'datapipe' }, hasCredential: false }],
    },
  },
]
