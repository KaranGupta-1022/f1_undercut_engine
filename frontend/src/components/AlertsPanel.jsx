import { useEffect } from 'react'
import './AlertsPanel.css'

const AUTO_DISMISS_MS = 30000

function urgencyClass(confidence) {
  if (confidence >= 0.8) return 'high'
  if (confidence >= 0.5) return 'medium'
  return 'low'
}

function AlertRow({ alert, onDismiss }) {
  useEffect(() => {
    const timer = setTimeout(() => onDismiss(alert.id), AUTO_DISMISS_MS)
    return () => clearTimeout(timer)
  }, [alert.id, onDismiss])

  const urgency = urgencyClass(alert.confidence)
  const isBoxNow = alert.recommendation === 'BOX NOW'

  return (
    <div className={`alert-row alert-${urgency}`}>
      <div className="alert-pair">
        <span className="alert-ahead">{alert.ahead}</span>
        <span className="alert-arrow">◂</span>
        <span className="alert-behind">{alert.behind}</span>
      </div>
      <div className="alert-metric">
        <span className="alert-metric-label">Gap</span>
        <span className="alert-metric-value">{alert.current_gap.toFixed(1)}s</span>
      </div>
      <div className="alert-metric">
        <span className="alert-metric-label">Δ/Lap</span>
        <span className="alert-metric-value">
          {alert.time_delta >= 0 ? '+' : ''}
          {alert.time_delta.toFixed(1)}s
        </span>
      </div>
      <div className="alert-metric">
        <span className="alert-metric-label">Conf</span>
        <span className="alert-metric-value">{Math.round(alert.confidence * 100)}%</span>
      </div>
      <button className={`alert-cta ${isBoxNow ? 'alert-cta-box' : 'alert-cta-stay'}`} disabled>
        {alert.recommendation}
      </button>
      <button className="alert-dismiss" onClick={() => onDismiss(alert.id)} aria-label="Dismiss alert">
        ×
      </button>
    </div>
  )
}

function AlertsPanel({ alerts, onDismiss }) {
  return (
    <div className="alerts-panel">
      <h2>
        <span className="panel-tick" />
        Undercut Alert Feed
      </h2>
      {alerts.length === 0 && <p className="alerts-empty">No undercut alerts yet</p>}
      <div className="alerts-list">
        {alerts.map((alert) => (
          <AlertRow key={alert.id} alert={alert} onDismiss={onDismiss} />
        ))}
      </div>
    </div>
  )
}

export default AlertsPanel
