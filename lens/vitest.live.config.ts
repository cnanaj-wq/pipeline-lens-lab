import { defineConfig } from 'vitest/config'

// Tests contre le vrai Airflow local : npm run test:live (rejeu réel : LENS_LIVE_REJEU=1).
export default defineConfig({
  test: { environment: 'node', include: ['tests/live/**/*.test.ts'], testTimeout: 30_000 },
})
