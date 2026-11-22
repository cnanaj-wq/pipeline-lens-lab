import '@testing-library/jest-dom/vitest'

// jsdom n'implémente pas le défilement.
if (typeof window !== 'undefined') window.scrollTo = () => {}
