const hasApiUrl = import.meta.env.VITE_API_URL !== undefined
const hasWsUrl = import.meta.env.VITE_WS_URL !== undefined

export const API_URL = hasApiUrl ? import.meta.env.VITE_API_URL : 'http://localhost:5000'
export const WS_URL = hasWsUrl ? import.meta.env.VITE_WS_URL : 'http://localhost:5000'
