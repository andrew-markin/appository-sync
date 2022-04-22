import { getToken } from './auth.js'

if (process.argv.length === 3) {
  const bucket = process.argv[2]
  console.log('Token for bucket:', bucket)
  console.log(getToken(bucket))
}
