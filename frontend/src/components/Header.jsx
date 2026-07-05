import { useEffect, useState } from 'react'
import axios from 'axios'
import './Header.css'

const TRACK_STATUS_LABELS = {
  GREEN: { label: 'TRACK CLEAR', className: 'clear' },
  YELLOW: { label: 'YELLOW FLAG', className: 'caution' },
  SAFETY_CAR: { label: 'SAFETY CAR', className: 'danger' },
  RED: { label: 'RED FLAG', className: 'danger' },
  'VSC Deployed': { label: 'VSC DEPLOYED', className: 'caution' },
}

function Header({ track, currentLap, connected, clientCount }) {
  const [messagesProcessed, setMessagesProcessed] = useState(0)
  const [trackStatus, setTrackStatus] = useState('GREEN')

  useEffect(() => {
    let cancelled = false

    async function poll() {
      try {
        const [statusRes, safetyCarRes] = await Promise.all([
          axios.get('/api/status'),
          axios.get('/api/safety-car-status'),
        ])
        if (cancelled) return
        setMessagesProcessed(statusRes.data.messages_processed)
        setTrackStatus(safetyCarRes.data.track_status)
      } catch {
        // Backend unreachable this tick — keep showing last known values
      }
    }

    poll()
    const interval = setInterval(poll, 4000)
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [])

  const statusMeta = TRACK_STATUS_LABELS[trackStatus] || TRACK_STATUS_LABELS.GREEN

  return (
    <header className="header">
      <div className="header-brand">
        <span className="header-flag" aria-hidden="true" />
        <div>
          <h1 className="header-title">Undercut Strategy Engine</h1>
          <p className="header-subtitle">Real-Time Telemetry · Kafka / Redis Pipeline</p>
        </div>
      </div>

      <div className="header-stats">
        <div className="header-stat">
          <span className="header-stat-label">Session</span>
          <span className="header-stat-value">{track || '—'}</span>
        </div>
        <div className="header-stat">
          <span className="header-stat-label">Lap</span>
          <span className="header-stat-value">{currentLap}</span>
        </div>
        <div className="header-stat">
          <span className="header-stat-label">Messages</span>
          <span className="header-stat-value">{messagesProcessed.toLocaleString()}</span>
        </div>
      </div>

      <div className="header-right">
        <span className={`track-pill track-pill-${statusMeta.className}`}>{statusMeta.label}</span>
        <div className="header-status">
          <span className={`dot ${connected ? 'connected' : 'disconnected'}`} />
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
          {clientCount != null && <span className="header-clients">· {clientCount} online</span>}
        </div>
      </div>
    </header>
  )
}

export default Header
