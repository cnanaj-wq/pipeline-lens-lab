import { motion, useReducedMotion } from 'motion/react'
import type { Bucket, Range } from '@/lib/history'

const SERIES = [
  { key: 'success', label: 'Réussies', color: 'var(--ok)' },
  { key: 'failed', label: 'En échec', color: 'var(--ko)' },
  { key: 'active', label: 'En cours', color: 'var(--run)' },
  { key: 'other', label: 'Autres', color: 'var(--wait)' },
] as const

const hf = new Intl.DateTimeFormat('fr-FR', { hour: '2-digit', minute: '2-digit' })
const dfm = new Intl.DateTimeFormat('fr-FR', { weekday: 'short', day: 'numeric' })

/** Activité : exécutions par heure (24 h) ou par jour (7 j), empilées par état. */
export function ActivityChart({ data, range }: { data: Bucket[]; range: Range }) {
  const reduce = useReducedMotion()
  const W = 720, H = 132, top = 8, bottom = 22
  const max = Math.max(1, ...data.map((b) => b.success + b.failed + b.active + b.other))
  const bw = W / data.length
  const label = (b: Bucket) => (range === '24h' ? hf.format(b.start) : dfm.format(b.start))
  const total = data.reduce((s, b) => s + b.success + b.failed + b.active + b.other, 0)

  return (
    <figure className="m-0">
      <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label={`Activité ${range === '24h' ? 'des dernières 24 heures' : 'des 7 derniers jours'} : ${total} exécutions`}>
        <line x1={0} x2={W} y1={H - bottom + 0.5} y2={H - bottom + 0.5} stroke="var(--border)" />
        {data.map((b, i) => {
          let y = H - bottom
          const x = i * bw + bw * 0.18
          const w = bw * 0.64
          const n = b.success + b.failed + b.active + b.other
          return (
            <g key={b.start}>
              <title>{`${label(b)} : ${n} exécution${n > 1 ? 's' : ''}${b.failed ? `, dont ${b.failed} en échec` : ''}${b.active ? `, ${b.active} en cours` : ''}`}</title>
              <rect x={i * bw} y={0} width={bw} height={H - bottom} fill="transparent" />
              {SERIES.map((s) => {
                const v = b[s.key]
                if (!v) return null
                const h = ((H - bottom - top) * v) / max
                y -= h
                return (
                  <motion.rect
                    key={`${s.key}-${v}`}
                    x={x} width={w} y={y} height={Math.max(1.5, h)} rx={2}
                    fill={s.color}
                    initial={reduce ? false : { scaleY: 0, opacity: 0.4 }}
                    animate={{ scaleY: 1, opacity: s.key === 'active' ? [1, 0.55, 1] : 1 }}
                    transition={s.key === 'active' && !reduce
                      ? { scaleY: { duration: 0.4, delay: i * 0.012 }, opacity: { duration: 1.6, repeat: Infinity } }
                      : { duration: 0.4, delay: reduce ? 0 : i * 0.012 }}
                    style={{ transformBox: 'fill-box', transformOrigin: 'bottom' }}
                  />
                )
              })}
              {(range === '7j' || i % 4 === 0) && (
                <text x={i * bw + bw / 2} y={H - 6} textAnchor="middle" fontSize={10} fill="var(--muted-foreground)">{label(b)}</text>
              )}
            </g>
          )
        })}
      </svg>
      <figcaption className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
        {SERIES.map((s) => (
          <span key={s.key} className="inline-flex items-center gap-1.5">
            <span aria-hidden className="size-2 rounded-sm" style={{ background: s.color }} />{s.label}
          </span>
        ))}
      </figcaption>
    </figure>
  )
}
