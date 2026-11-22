import { describe, expect, it } from 'vitest'
import { cronToFrench, fmtAgo, fmtCountdown, fmtDuration, fmtDurationPrecise, fmtInt, humanize, stateInfo } from './format'

// Espaces insécables du format français normalisées pour la lisibilité des attentes.
const n = (s: string) => s.replace(/[  ]/g, ' ')

describe('formats', () => {
  it('entiers à la française', () => expect(n(fmtInt(11210))).toBe('11 210'))
  it('durées', () => {
    expect(fmtDuration(null)).toBe('—')
    expect(fmtDuration(0.4)).toBe('< 1 s')
    expect(fmtDuration(42)).toBe('42 s')
    expect(fmtDuration(125)).toBe('2 min 5 s')
    expect(fmtDuration(3 * 3600 + 7 * 60)).toBe('3 h 07')
  })
  it('durées précises', () => {
    expect(fmtDurationPrecise(0.4237)).toBe('0,42 s')
    expect(fmtDurationPrecise(12.74)).toBe('12,7 s')
    expect(fmtDurationPrecise(125)).toBe('2 min 5 s')
  })
  it('comptes à rebours et ancienneté', () => {
    const now = Date.parse('2026-11-10T12:00:00Z')
    expect(fmtCountdown(null, now)).toBe('aucune prévue')
    expect(fmtCountdown('2026-11-10T11:00:00Z', now)).toBe('imminente')
    expect(fmtCountdown('2026-11-10T12:30:00Z', now)).toBe('dans 30 min')
    expect(fmtCountdown('2026-11-10T14:05:00Z', now)).toBe('dans 2 h 05')
    expect(fmtAgo('2026-11-10T09:00:00Z', now)).toBe('il y a 3 h')
  })
  it('cron en français', () => {
    expect(cronToFrench('0 9 * * *')).toBe('Tous les jours à 9 h')
    expect(cronToFrench('30 7 * * 1-5')).toBe('Du lundi au vendredi à 7 h 30')
    expect(cronToFrench('*/15 * * * *')).toBe('Toutes les 15 minutes')
    expect(cronToFrench('@daily')).toBe('Tous les jours à minuit')
    expect(cronToFrench('0 9 1 1 *')).toBeNull()
  })
  it('états et libellés', () => {
    expect(stateInfo('up_for_retry').tone).toBe('warn')
    expect(stateInfo(null).short).toBe('À venir')
    expect(humanize('transform.transform_produits')).toBe('Produits')
    expect(humanize('extract.load_src_data')).toBe('Load src data')
  })
})
