import { useEffect, useState } from 'react'
import axios from 'axios'
import { getDriverColor, getDriverTeam } from '../utils/drivers'
import './SelectedDriverPanel.css'

const CONDITION_LABELS = {
  dry: 'Dry',
  rain: 'Rain',
  hot: 'Hot',
  cool: 'Cool',
}

const ASSUMED_MAX_TYRE_LIFE = 30

function degradationPercent(tyreLife) {
  if (tyreLife == null) return 0
  return Math.min(100, Math.round((tyreLife / ASSUMED_MAX_TYRE_LIFE) * 100))
}

function SelectedDriverPanel({ driver, stats }) {
  const [projectedPace, setProjectedPace] = useState(null)
  const [weather, setWeather] = useState(null)

  useEffect(() => {
    if (!driver) return
    let cancelled = false

    async function poll() {
      try {
        const [paceRes, weatherRes] = await Promise.all([
          axios.get(`/api/projected-pace/${driver}`, { params: { laps: 1 } }),
          axios.get(`/api/weather/${driver}`),
        ])
        if (cancelled) return
        setProjectedPace(paceRes.data.projected_pace)
        setWeather(weatherRes.data)
      } catch {
        // No projection/weather available yet for this driver — keep prior values
      }
    }

    poll()
    const interval = setInterval(poll, 4000)
    return () => {
      cancelled = true
      clearInterval(interval)
    }
  }, [driver])

  if (!driver || !stats) {
    return (
      <div className="selected-driver-card">
        <h2>
          <span className="panel-tick" />
          Selected Driver
        </h2>
        <p className="selected-driver-empty">Click a driver in Standings to inspect them here</p>
      </div>
    )
  }

  const color = getDriverColor(driver)
  const team = getDriverTeam(driver)
  const degradation = degradationPercent(stats.tyre_life)
  const weatherData = weather?.weather_data || {}
  const condition = weather?.condition ? CONDITION_LABELS[weather.condition] || weather.condition : '—'

  return (
    <div className="selected-driver-card">
      <h2>
        <span className="panel-tick" />
        Selected Driver
      </h2>

      <div className="selected-driver-heading">
        <span className="selected-driver-code" style={{ color }}>
          {driver}
        </span>
        <span className="selected-driver-position">P{stats.position ?? '—'}</span>
      </div>
      <p className="selected-driver-team">{team}</p>

      <div className="selected-driver-stats">
        <div className="selected-driver-row">
          <span>Current Pace</span>
          <span>{stats.current_pace ? `${stats.current_pace.toFixed(1)}s` : '—'}</span>
        </div>
        <div className="selected-driver-row">
          <span>Tire Compound</span>
          <span className="selected-driver-compound">{stats.compound ?? '—'}</span>
        </div>
        <div className="selected-driver-row">
          <span>Tire Age</span>
          <span>{stats.tyre_life != null ? `${stats.tyre_life} laps` : '—'}</span>
        </div>
        <div className="selected-driver-row">
          <span>Projected Next Lap</span>
          <span>{projectedPace ? `${projectedPace.toFixed(1)}s` : '—'}</span>
        </div>
      </div>

      <div className="degradation-block">
        <span className="degradation-label">Tire Degradation</span>
        <div className="degradation-bar">
          <div className="degradation-marker" style={{ left: `${degradation}%` }} />
        </div>
      </div>

      <h3 className="conditions-title">Track Conditions</h3>
      <div className="conditions-grid">
        <div className="condition-tile">
          <span className="condition-value">
            {weatherData.tracktemp != null ? `${Math.round(weatherData.tracktemp)}°C` : '—'}
          </span>
          <span className="condition-label">Track Temp</span>
        </div>
        <div className="condition-tile">
          <span className="condition-value">
            {weatherData.airtemp != null ? `${Math.round(weatherData.airtemp)}°C` : '—'}
          </span>
          <span className="condition-label">Air Temp</span>
        </div>
        <div className="condition-tile">
          <span className="condition-value">{condition}</span>
          <span className="condition-label">Condition</span>
        </div>
        <div className="condition-tile">
          <span className="condition-value">
            {weatherData.windspeed != null ? `${weatherData.windspeed} km/h` : '—'}
          </span>
          <span className="condition-label">Wind</span>
        </div>
      </div>
    </div>
  )
}

export default SelectedDriverPanel
