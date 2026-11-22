import { useLayoutEffect, useRef } from 'react'
import { animate, useReducedMotion } from 'motion/react'

/**
 * Nombre qui « compte » jusqu'à sa nouvelle valeur quand les données changent
 * (depuis 0 au premier affichage). Sans animation si l'utilisateur réduit les mouvements.
 * Le texte est écrit directement dans le DOM : aucun rendu React par image.
 */
export function AnimatedNumber({ value, format = defaultFormat, className }: {
  value: number
  format?: (n: number) => string
  className?: string
}) {
  const ref = useRef<HTMLSpanElement>(null)
  const prev = useRef(0)
  const fmt = useRef(format)
  fmt.current = format
  const reduce = useReducedMotion()

  useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const from = prev.current
    prev.current = value
    if (reduce || from === value) {
      el.textContent = fmt.current(value)
      return
    }
    el.textContent = fmt.current(from)
    const c = animate(from, value, { duration: 0.7, ease: 'easeOut', onUpdate: (v) => { el.textContent = fmt.current(v) } })
    return () => { c.stop(); el.textContent = fmt.current(value) }
  }, [value, reduce])

  return <span ref={ref} className={className} />
}

const defaultFormat = (n: number) => new Intl.NumberFormat('fr-FR').format(Math.round(n))
