import { Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/Layout'
import RequireAuth from './components/RequireAuth'
import Login from './pages/Login'
import Projects from './pages/Projects'
import ProjectDetail from './pages/ProjectDetail'
import FlowEditor from './pages/FlowEditor'
import FlowExecutions from './pages/FlowExecutions'
import ExecutionDetail from './pages/ExecutionDetail'
import FlowDeadLetters from './pages/FlowDeadLetters'

function App() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route element={<RequireAuth />}>
        <Route element={<Layout />}>
          <Route path="/" element={<Navigate to="/projects" replace />} />
          <Route path="/projects" element={<Projects />} />
          <Route path="/projects/:projectId" element={<ProjectDetail />} />
          <Route path="/projects/:projectId/flows/:flowId" element={<FlowEditor />} />
          <Route path="/projects/:projectId/flows/:flowId/executions" element={<FlowExecutions />} />
          <Route path="/projects/:projectId/flows/:flowId/executions/:executionId" element={<ExecutionDetail />} />
          <Route path="/projects/:projectId/flows/:flowId/dead-letters" element={<FlowDeadLetters />} />
        </Route>
      </Route>
    </Routes>
  )
}

export default App
