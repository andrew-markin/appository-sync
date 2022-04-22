import jwt from 'jsonwebtoken'

const SECRET = process.env.TOKEN_SECRET || 'DEFAULT_SECRET'

export const getToken = (bucket) => {
  return jwt.sign({ bucket }, SECRET)
}

export const getBucket = (token) => {
  try {
    return jwt.verify(token, SECRET).bucket
  } catch {}
}
