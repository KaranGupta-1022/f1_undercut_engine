import { useEffect, useState, useCallback } from 'react'
import { useSocket } from './hooks/useSocket'
import Header from './components/Header'
import LiveChart from './components/LiveChart'
import Standings from './components/Standings'
import CircuitMap from './components/CircuitMap'
import AlertsPanel from './components/AlertsPanel'
import SelectedDriverPanel from './components/SelectedDriverPanel'
import './App.css'

function App() {
  const { socket, connected } = useSocket()
  const [raceData, setRaceData] = useState({})
  const [driverStats, setDriverStats] = useState({})
  const [currentLap, setCurrentLap] = useState(0)
  const [alerts, setAlerts] = useState([])
  const [track, setTrack] = useState(null)
  const [clientCount, setClientCount] = useState(null)
  const [hoveredDriver, setHoveredDriver] = useState(null)
  const [selectedDriver, setSelectedDriver] = useState(null)

  const handleDismissAlert = useCallback((id) => {
    setAlerts((prev) => prev.filter((alert) => alert.id !== id))
  }, [])

  const handleSelectDriver = useCallback((driver) => {
    setSelectedDriver(driver)
  }, [])

  useEffect(() => {
    if (!socket) return

    function handleRaceUpdate(data) {
      const { driver, lap_number, lap_time, position, compound, tyre_life, current_pace } = data

      setRaceData((prev) => {
        const existing = prev[driver] || []
        const withoutLap = existing.filter((point) => point.lap_number !== lap_number)
        const updated = [...withoutLap, { lap_number, lap_time }]
        updated.sort((a, b) => a.lap_number - b.lap_number)
        return { ...prev, [driver]: updated }
      })

      setDriverStats((prev) => ({
        ...prev,
        [driver]: { position, compound, tyre_life, current_pace, lap_time, lap_number },
      }))

      setCurrentLap((prev) => Math.max(prev, lap_number || 0))
    }

    function handleUndercutAlert(data) {
      const alert = { ...data, id: crypto.randomUUID() }
      setAlerts((prev) => [alert, ...prev])
    }

    function handleTrackInfo(data) {
      setTrack(data.track)
    }

    function handleConnectionResponse(data) {
      setTrack((prev) => prev || data.track)
    }

    function handleClientCount(data) {
      setClientCount(data.count)
    }

    socket.on('race_update', handleRaceUpdate)
    socket.on('undercut_alert', handleUndercutAlert)
    socket.on('track_info', handleTrackInfo)
    socket.on('connection_response', handleConnectionResponse)
    socket.on('client_count', handleClientCount)

    return () => {
      socket.off('race_update', handleRaceUpdate)
      socket.off('undercut_alert', handleUndercutAlert)
      socket.off('track_info', handleTrackInfo)
      socket.off('connection_response', handleConnectionResponse)
      socket.off('client_count', handleClientCount)
    }
  }, [socket])

  useEffect(() => {
    if (selectedDriver) return
    const leader = Object.entries(driverStats).find(([, stats]) => stats.position === 1)
    if (leader) setSelectedDriver(leader[0])
  }, [driverStats, selectedDriver])

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <Header
          track={track}
          currentLap={currentLap}
          connected={connected}
          clientCount={clientCount}
        />
      </div>

      <div className="dashboard-left">
        <Standings
          driverStats={driverStats}
          hoveredDriver={hoveredDriver}
          onHoverDriver={setHoveredDriver}
          onSelectDriver={handleSelectDriver}
        />
      </div>

      <main className="dashboard-main">
        <LiveChart
          raceData={raceData}
          hoveredDriver={hoveredDriver}
          onHoverDriver={setHoveredDriver}
        />
        <AlertsPanel alerts={alerts} onDismiss={handleDismissAlert} />
      </main>

      <aside className="dashboard-right">
        <SelectedDriverPanel driver={selectedDriver} stats={driverStats[selectedDriver]} />
        <CircuitMap track={track} />
      </aside>
    </div>
  )
}

export default App
