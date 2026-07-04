import { useEffect, useRef, useState } from 'react'
import { io } from 'socket.io-client'
import { WS_URL } from '../config'

export function useSocket() {
  const socketRef = useRef(null)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    const socket = io(WS_URL, {
      transports: ['websocket', 'polling'],
    })
    socketRef.current = socket

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
  }, [])

  return { socket: socketRef.current, connected }
}
