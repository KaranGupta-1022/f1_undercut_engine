const CIRCUIT_ENTRIES = [
  { slug: 'monaco-6', keywords: ['monaco'] },
  { slug: 'bahrain-1', keywords: ['bahrain'] },
  { slug: 'jeddah-1', keywords: ['saudi', 'jeddah'] },
  { slug: 'melbourne-2', keywords: ['australia', 'melbourne'] },
  { slug: 'suzuka-2', keywords: ['japan', 'suzuka'] },
  { slug: 'shanghai-1', keywords: ['china', 'shanghai'] },
  { slug: 'miami-1', keywords: ['miami'] },
  { slug: 'imola-3', keywords: ['emilia romagna', 'imola'] },
  { slug: 'montreal-6', keywords: ['canada', 'montreal', 'gilles villeneuve'] },
  { slug: 'catalunya-6', keywords: ['spain', 'spanish', 'catalunya', 'barcelona'] },
  { slug: 'spielberg-3', keywords: ['austria', 'austrian', 'red bull ring', 'spielberg'] },
  { slug: 'silverstone-8', keywords: ['british', 'silverstone', 'united kingdom'] },
  { slug: 'hungaroring-3', keywords: ['hungary', 'hungarian', 'hungaroring'] },
  { slug: 'spa-francorchamps-4', keywords: ['belgium', 'belgian', 'spa'] },
  { slug: 'zandvoort-5', keywords: ['dutch', 'netherlands', 'zandvoort'] },
  { slug: 'monza-7', keywords: ['italy', 'italian', 'monza'] },
  { slug: 'baku-1', keywords: ['azerbaijan', 'baku'] },
  { slug: 'marina-bay-4', keywords: ['singapore', 'marina bay'] },
  { slug: 'austin-1', keywords: ['united states', 'austin', 'americas', 'cota'] },
  { slug: 'mexico-city-3', keywords: ['mexico'] },
  { slug: 'interlagos-2', keywords: ['brazil', 'brazilian', 'sao paulo', 'interlagos'] },
  { slug: 'las-vegas-1', keywords: ['las vegas'] },
  { slug: 'lusail-1', keywords: ['qatar', 'lusail'] },
  { slug: 'yas-marina-2', keywords: ['abu dhabi', 'yas marina'] },
]

const BASE_URL =
  'https://raw.githubusercontent.com/julesr0y/f1-circuits-svg/main/circuits/minimal/white-outline'

export function getCircuitSvgUrl(trackName) {
  if (!trackName) return null
  const normalized = trackName.toLowerCase()
  const match = CIRCUIT_ENTRIES.find((entry) =>
    entry.keywords.some((keyword) => normalized.includes(keyword))
  )
  return match ? `${BASE_URL}/${match.slug}.svg` : null
}
