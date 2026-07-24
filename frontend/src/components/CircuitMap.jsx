import { useState } from 'react'
import { getCircuitSvgUrl } from '../utils/circuits'
import './CircuitMap.css'

function CircuitMap({ track }) {
  const [imageFailed, setImageFailed] = useState(false)
  const svgUrl = getCircuitSvgUrl(track)
  const showFallback = !svgUrl || imageFailed

  const [prevSvgUrl, setPrevSvgUrl] = useState(svgUrl)
  if (svgUrl !== prevSvgUrl) {
    setPrevSvgUrl(svgUrl)
    setImageFailed(false)
  }

  return (
    <div className="circuit-card">
      <h2>
        <span className="panel-tick" />
        Circuit — {track ? track.toUpperCase() : 'UNKNOWN'}
      </h2>

      {showFallback ? (
        <svg className="circuit-svg" viewBox="0 0 240 160" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path
            d="M30 40 C10 60, 10 90, 40 100 C70 110, 60 60, 90 55 C120 50, 110 100, 150 105 C190 110, 200 60, 170 40 C150 27, 130 45, 110 35 C90 25, 60 20, 30 40 Z"
            stroke="#e10600"
            strokeWidth="3"
            strokeLinecap="round"
            strokeDasharray="2 8"
          />
        </svg>
      ) : (
        <img
          className="circuit-svg"
          src={svgUrl}
          alt={`${track} circuit layout`}
          onError={() => setImageFailed(true)}
        />
      )}

      {!showFallback && (
        <p className="circuit-credit">
          Circuit maps by{' '}
          <a href="https://github.com/julesr0y/f1-circuits-svg" target="_blank" rel="noreferrer">
            f1-circuits-svg
          </a>{' '}
          (CC BY 4.0)
        </p>
      )}
    </div>
  )
}

export default CircuitMap
