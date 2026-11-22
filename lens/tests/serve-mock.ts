/** Lance le faux Airflow pour les tests E2E : node tests/serve-mock.ts [port] */
import { startMockAirflow } from './mock-airflow.ts'

const port = Number(process.argv[2] ?? 18085)
const m = await startMockAirflow(port)
console.log(`Faux Airflow prêt sur ${m.url}`)
