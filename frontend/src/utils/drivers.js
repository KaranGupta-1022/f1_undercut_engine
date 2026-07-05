export const DRIVER_COLORS = {
  VER: '#3671C6',
  HAM: '#27F4D2',
  LEC: '#E8002D',
  NOR: '#FF8000',
  RUS: '#24FFFF',
  SAI: '#FF8000',
  PER: '#3671C6',
  ALO: '#229971',
  PIA: '#FF8000',
  STR: '#229971',
  GAS: '#FF87BC',
  OCO: '#FF87BC',
  ALB: '#64C4FF',
  TSU: '#6692FF',
  HUL: '#B6BABD',
  MAG: '#B6BABD',
  BOT: '#52E252',
  ZHO: '#52E252',
  RIC: '#6692FF',
  SAR: '#64C4FF',
}

export const DRIVER_TEAMS = {
  VER: 'Red Bull Racing',
  PER: 'Red Bull Racing',
  HAM: 'Mercedes',
  RUS: 'Mercedes',
  LEC: 'Ferrari',
  SAI: 'Ferrari',
  NOR: 'McLaren',
  PIA: 'McLaren',
  ALO: 'Aston Martin',
  STR: 'Aston Martin',
  GAS: 'Alpine',
  OCO: 'Alpine',
  TSU: 'RB F1 Team',
  RIC: 'RB F1 Team',
  BOT: 'Kick Sauber',
  ZHO: 'Kick Sauber',
  HUL: 'Haas F1 Team',
  MAG: 'Haas F1 Team',
  ALB: 'Williams',
  SAR: 'Williams',
}

const FALLBACK_PALETTE = ['#f4c542', '#8e44ad', '#16a085', '#d35400', '#2980b9', '#c0392b']

export function getDriverColor(driver, index = 0) {
  return DRIVER_COLORS[driver] || FALLBACK_PALETTE[index % FALLBACK_PALETTE.length]
}

export function getDriverTeam(driver) {
  return DRIVER_TEAMS[driver] || 'Unknown Team'
}
