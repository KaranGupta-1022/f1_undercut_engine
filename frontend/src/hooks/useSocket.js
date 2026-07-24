import { useEffect, useState } from 'react'
import { io } from 'socket.io-client'
import { WS_URL } from '../config'

export function useSocket() {
  const [socket] = useState(() => io(WS_URL, { transports: ['websocket', 'polling'] }))
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    function handleConnect() {
      setConnected(true)
    }

    function handleDisconnect() {
      setConnected(false)
    }

    socket.on('connect', handleConnect)
    socket.on('disconnect', handleDisconnect)

    return () => {
      socket.off('connect', handleConnect)
      socket.off('disconnect', handleDisconnect)
      socket.disconnect()
    }
  }, [socket])

  return { socket, connected }
}
