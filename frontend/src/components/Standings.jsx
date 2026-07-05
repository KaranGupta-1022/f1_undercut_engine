import './Standings.css'

const COMPOUND_COLORS = {
  SOFT: '#e10600',
  MEDIUM: '#ffd12e',
  HARD: '#f0f0f0',
  INTERMEDIATE: '#3cb44b',
  WET: '#0072ce',
}

function compoundColor(compound) {
  return COMPOUND_COLORS[compound] || '#8a92b2'
}

function sortedDrivers(driverStats) {
  return Object.entries(driverStats).sort(([, a], [, b]) => {
    const posA = a.position ?? 999
    const posB = b.position ?? 999
    return posA - posB
  })
}

function Standings({ driverStats, hoveredDriver, onHoverDriver, onSelectDriver }) {
  const entries = sortedDrivers(driverStats)

  if (entries.length === 0) {
    return (
      <div className="standings-card">
        <h2>
            <span className="panel-tick" />
            Live Standings
        </h2>
        <p className="standings-empty">Waiting for race data...</p>
      </div>
    )
  }

  return (
    <div className="standings-card">
        <h2>
            <span className="panel-tick" />
            Live Standings
        </h2>
      <div className="standings-table-wrap">
        <table className="standings-table">
          <thead>
            <tr>
              <th>Pos</th>
              <th>Driver</th>
              <th>Tyre</th>
              <th>Age</th>
              <th>Last Lap</th>
              <th>Pace</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([driver, stats]) => (
              <tr
                key={driver}
                className={hoveredDriver === driver ? 'standings-row-active' : ''}
                onMouseEnter={() => onHoverDriver(driver)}
                onMouseLeave={() => onHoverDriver(null)}
                onClick={() => onSelectDriver(driver)}
              >
                <td>{stats.position ?? '—'}</td>
                <td className="standings-driver">{driver}</td>
                <td>
                  <span
                    className="compound-badge"
                    style={{ backgroundColor: compoundColor(stats.compound) }}
                  >
                    {stats.compound ?? '—'}
                  </span>
                </td>
                <td>{stats.tyre_life ?? '—'}</td>
                <td>{stats.lap_time != null ? stats.lap_time.toFixed(3) : '—'}</td>
                <td>{stats.current_pace ? stats.current_pace.toFixed(2) : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default Standings
