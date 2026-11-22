import type { State } from './api'

const nf = new Intl.NumberFormat('fr-FR')
export const fmtInt = (n: number) => nf.format(Math.round(n))

export function fmtDuration(sec: number | null | undefined): string {
  if (sec == null || !Number.isFinite(sec)) return '—'
  if (sec < 1) return '< 1 s'
  if (sec < 60) return `${Math.round(sec)} s`
  const m = Math.floor(sec / 60)
  const s = Math.round(sec % 60)
  if (m < 60) return s ? `${m} min ${s} s` : `${m} min`
  const h = Math.floor(m / 60)
  return `${h} h ${String(m % 60).padStart(2, '0')}`
}

/** Durée précise pour le détail : « 0,42 s », « 12,7 s », puis comme fmtDuration. */
export function fmtDurationPrecise(sec: number | null | undefined): string {
  if (sec == null || !Number.isFinite(sec)) return '—'
  if (sec < 10) return `${sec.toFixed(2).replace('.', ',')} s`
  if (sec < 60) return `${sec.toFixed(1).replace('.', ',')} s`
  return fmtDuration(sec)
}

const tf = new Intl.DateTimeFormat('fr-FR', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
const df = new Intl.DateTimeFormat('fr-FR', { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })
export const fmtTime = (iso: string | null) => (iso ? tf.format(new Date(iso)) : '—')
export const fmtDateTime = (iso: string | null) => (iso ? df.format(new Date(iso)) : '—')

export function fmtCountdown(iso: string | null, now: number): string {
  if (!iso) return 'aucune prévue'
  const diff = new Date(iso).getTime() - now
  if (diff <= 0) return 'imminente'
  const min = Math.floor(diff / 60_000)
  if (min < 1) return "dans moins d'une minute"
  if (min < 60) return `dans ${min} min`
  const h = Math.floor(min / 60)
  if (h < 48) return `dans ${h} h ${String(min % 60).padStart(2, '0')}`
  return `dans ${Math.round(h / 24)} jours`
}

export function fmtAgo(iso: string | null, now: number): string {
  if (!iso) return '—'
  const min = Math.round((now - new Date(iso).getTime()) / 60_000)
  if (min < 1) return "à l'instant"
  if (min < 60) return `il y a ${min} min`
  const h = Math.round(min / 60)
  if (h < 48) return `il y a ${h} h`
  return `il y a ${Math.round(h / 24)} j`
}

const JOURS = ['dimanche', 'lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi']

/** Traduit les expressions cron courantes en français ; sinon renvoie null. */
export function cronToFrench(cron: string | null): string | null {
  if (!cron) return null
  const presets: Record<string, string> = {
    '@daily': 'Tous les jours à minuit', '@hourly': 'Toutes les heures', '@weekly': 'Chaque dimanche à minuit',
    '@monthly': 'Le 1er de chaque mois', '@once': 'Une seule fois', 'None': 'Déclenchement manuel',
  }
  if (presets[cron]) return presets[cron]
  const p = cron.trim().split(/\s+/)
  if (p.length !== 5) return null
  const [mi, h, dom, mon, dow] = p
  if (!/^\d+$/.test(mi) || !/^\d+$/.test(h) || mon !== '*') {
    if (/^\*\/\d+$/.test(mi) && h === '*' && dom === '*' && dow === '*') return `Toutes les ${mi.slice(2)} minutes`
    if (/^\d+$/.test(mi) && h === '*' && dom === '*' && dow === '*') return `Toutes les heures à ${mi.padStart(2, '0')}`
    return null
  }
  const heure = `${Number(h)} h${Number(mi) ? ' ' + mi.padStart(2, '0') : ''}`
  if (dom === '*' && dow === '*') return `Tous les jours à ${heure}`
  if (dom === '*' && dow === '1-5') return `Du lundi au vendredi à ${heure}`
  if (dom === '*' && /^\d$/.test(dow)) return `Chaque ${JOURS[Number(dow) % 7]} à ${heure}`
  if (/^\d+$/.test(dom) && dow === '*') return `Le ${dom === '1' ? '1er' : dom} de chaque mois à ${heure}`
  return null
}

export type Tone = 'ok' | 'ko' | 'run' | 'wait' | 'warn'

export const STATE_LABEL: Record<string, { label: string; short: string; tone: Tone }> = {
  success: { label: 'Réussi', short: 'Réussi', tone: 'ok' },
  failed: { label: 'Échec', short: 'Échec', tone: 'ko' },
  running: { label: 'En cours', short: 'En cours', tone: 'run' },
  queued: { label: 'En file', short: 'En file', tone: 'wait' },
  scheduled: { label: 'Planifié', short: 'Planifié', tone: 'wait' },
  upstream_failed: { label: 'Bloqué par un échec en amont', short: 'Bloqué', tone: 'warn' },
  skipped: { label: 'Ignoré', short: 'Ignoré', tone: 'wait' },
  up_for_retry: { label: 'Nouvel essai prévu', short: 'Nouvel essai', tone: 'warn' },
  up_for_reschedule: { label: 'Replanifié', short: 'Replanifié', tone: 'wait' },
  deferred: { label: 'En attente d’un événement', short: 'En attente', tone: 'run' },
  restarting: { label: 'Redémarrage', short: 'Redémarrage', tone: 'run' },
  removed: { label: 'Retiré', short: 'Retiré', tone: 'wait' },
}

export function stateInfo(s: State | undefined) {
  if (!s) return { label: 'Pas encore exécuté', short: 'À venir', tone: 'wait' as Tone }
  return STATE_LABEL[s] ?? { label: s, short: s, tone: 'wait' as Tone }
}

/** Libellé lisible : « transform.transform_produits » → « Transform produits ». */
export function humanize(taskId: string): string {
  const parts = taskId.split('.')
  const leaf = parts.pop() ?? taskId
  const group = parts.pop()
  // « transform.transform_produits » → « Produits » ; « extract.load_src_data » garde son verbe.
  const s = group && leaf.toLowerCase().startsWith(group.toLowerCase() + '_') ? leaf.slice(group.length + 1) : leaf
  const words = s.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[_-]+/g, ' ').trim().toLowerCase()
  return words.charAt(0).toUpperCase() + words.slice(1)
}
