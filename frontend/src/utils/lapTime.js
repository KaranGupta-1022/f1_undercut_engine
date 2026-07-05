export function formatLapTime(seconds) {
  if (seconds == null || Number.isNaN(seconds)) return '—'

  const minutes = Math.floor(seconds / 60)
  const remainingSeconds = seconds - minutes * 60

  if (minutes > 0) {
    return `${minutes}:${remainingSeconds.toFixed(3).padStart(6, '0')}`
  }

  return remainingSeconds.toFixed(3)
}
