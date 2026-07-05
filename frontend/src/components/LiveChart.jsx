import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { formatLapTime } from '../utils/lapTime'
import { getDriverColor } from '../utils/drivers'
import './LiveChart.css'

const MAX_LAPS = 30

function buildChartData(raceData) {
  const lapMap = new Map()

  for (const driver of Object.keys(raceData)) {
    const points = raceData[driver].slice(-MAX_LAPS)
    for (const point of points) {
      if (!lapMap.has(point.lap_number)) {
        lapMap.set(point.lap_number, { lap_number: point.lap_number })
      }
      lapMap.get(point.lap_number)[driver] = point.lap_time
    }
  }

  return Array.from(lapMap.values()).sort((a, b) => a.lap_number - b.lap_number)
}

function CustomTooltip({ active, payload, label, hoveredDriver }) {
  if (!active || !hoveredDriver || !payload || payload.length === 0) return null

  const entry = payload.find((p) => p.dataKey === hoveredDriver)
  if (!entry || entry.value == null) return null

  return (
    <div className="chart-tooltip" style={{ borderLeftColor: entry.color }}>
      <div className="chart-tooltip-driver" style={{ color: entry.color }}>
        {entry.dataKey}
      </div>
      <div className="chart-tooltip-lap">Lap {label}</div>
      <div className="chart-tooltip-time">{formatLapTime(entry.value)}</div>
    </div>
  )
}

function LiveChart({ raceData, hoveredDriver, onHoverDriver }) {
  const drivers = Object.keys(raceData)

  if (drivers.length === 0) {
    return (
      <div className="chart-card">
        <h2>
          <span className="panel-tick" />
          Lap Times
        </h2>
        <p className="chart-empty">Waiting for race data...</p>
      </div>
    )
  }

  const chartData = buildChartData(raceData)

  return (
    <div className="chart-card">
      <h2>
        <span className="panel-tick" />
        Lap Times
      </h2>
      <ResponsiveContainer width="100%" height={420}>
        <LineChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#232a5c" />
          <XAxis
            dataKey="lap_number"
            stroke="#8a92b2"
            label={{ value: 'Lap', position: 'insideBottom', offset: -5, fill: '#8a92b2' }}
          />
          <YAxis
            stroke="#8a92b2"
            label={{ value: 'Lap Time (s)', angle: -90, position: 'insideLeft', fill: '#8a92b2' }}
            domain={['auto', 'auto']}
          />
          <Tooltip content={<CustomTooltip hoveredDriver={hoveredDriver} />} />
          <Legend
            onMouseEnter={(entry) => onHoverDriver(entry.dataKey)}
            onMouseLeave={() => onHoverDriver(null)}
          />
          {drivers.map((driver, index) => {
            const isHovered = hoveredDriver === driver
            const isDimmed = hoveredDriver && !isHovered
            return (
              <Line
                key={driver}
                type="monotone"
                dataKey={driver}
                stroke={getDriverColor(driver, index)}
                strokeWidth={isHovered ? 3 : 1.5}
                strokeOpacity={isDimmed ? 0.15 : 1}
                dot={false}
                connectNulls
                isAnimationActive={false}
                onMouseEnter={() => onHoverDriver(driver)}
                onMouseLeave={() => onHoverDriver(null)}
              />
            )
          })}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default LiveChart
