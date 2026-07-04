import { useEffect, useState } from 'react'
import axios from 'axios'
import { useSocket } from './hooks/useSocket'
import './App.css'

function App() {
  const { socket, connected } = useSocket()
  const [track, setTrack] = useState(null)
  const [clientCount, setClientCount] = useState(null)
  const [status, setStatus] = useState(null)
  const [statusError, setStatusError] = useState(null)

  useEffect(() => {
    if (!socket) return

    function handleConnectionResponse(data) {
      console.log('connection_response:', data)
      setTrack(data.track)
    }

    function handleClientCount(data) {
      console.log('client_count:', data)
      setClientCount(data.count)
    }

    socket.on('connection_response', handleConnectionResponse)
    socket.on('client_count', handleClientCount)

    return () => {
      socket.off('connection_response', handleConnectionResponse)
      socket.off('client_count', handleClientCount)
    }
  }, [socket])

  useEffect(() => {
    axios
      .get('/api/status')
      .then((res) => setStatus(res.data))
      .catch((err) => setStatusError(err.message))
  }, [])

  return (
    <div className="app">
      <h1>F1 Undercut Engine — Frontend Ready</h1>
      <p className="subtitle">Phase 9: frontend scaffolding + backend smoke test</p>

      <div className="card">
        <div className="status-row">
          <span className={`dot ${connected ? 'connected' : 'disconnected'}`} />
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
        </div>
        {connected && (
          <p className="meta">
            Track: {track || 'not set'} · Clients online: {clientCount ?? '—'}
          </p>
        )}
      </div>

      <div className="card">
        <h2>/api/status</h2>
        {statusError && <p className="error-text">Error: {statusError}</p>}
        {!statusError && !status && <p className="meta">Loading...</p>}
        {status && (
          <div className="stats-grid">
            <div>
              <div className="stat-label">Status</div>
              <div className="stat-value">{status.status}</div>
            </div>
            <div>
              <div className="stat-label">Current Lap</div>
              <div className="stat-value">{status.current_lap}</div>
            </div>
            <div>
              <div className="stat-label">Drivers Tracked</div>
              <div className="stat-value">{status.drivers_tracked}</div>
            </div>
            <div>
              <div className="stat-label">Messages Processed</div>
              <div className="stat-value">{status.messages_processed}</div>
            </div>
          </div>
        )}
      </div>

      <p className="next-phase">Next: Phase 10 — build dashboard UI</p>
    </div>
  )
}

export default App
