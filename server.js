import { fileURLToPath } from 'url'
import { getBucket } from './auth.js'
import { Server } from 'socket.io'
import AsyncLock from 'async-lock'
import fs from 'fs-extra'
import Joi from 'joi'
import mortice from 'mortice'
import path from 'path'

const execLock = mortice('exec-lock', { timeout: 30000 })

// Storage routines

const docLock = new AsyncLock()

const modulePath = fileURLToPath(import.meta.url)
const moduleDir = path.dirname(modulePath)

const storageDir = process.env.STORAGE || path.join(moduleDir, './storage')
fs.ensureDirSync(storageDir)

const server = new Server({ cors: { origin: true } })

server.use((socket, next) => {
  socket.bucket = getBucket(socket.handshake.auth.token)
  if (!socket.bucket) return next(new Error('Access denied'))
  socket.ip = socket.handshake.headers['x-real-ip']
  next()
})

const subscriptions = new Map()

const refSchema = Joi.string().allow(null).lowercase().hex().length(64)
const dataSchema = Joi.string().base64().max(1024 * 1024)
const versionSchema = Joi.number().optional().min(1)

server.on('connection', (socket) => {
  console.log(`Socket ${socket.id} connected`)

  const assertRef = () => {
    if (!socket.ref) throw new Error('Reference is not provided')
  }

  const getKey = () => {
    return `${socket.bucket}:${socket.ref}`
  }

  const subscribe = (ref) => {
    unsubscribe()
    socket.ref = ref
    if (!ref) return
    const key = getKey()
    const sockets = subscriptions.get(key) || []
    sockets.push(socket)
    subscriptions.set(key, sockets)
  }

  const unsubscribe = () => {
    if (!socket.ref) return
    const key = getKey()
    let sockets = subscriptions.get(key) || []
    sockets = sockets.filter((item) => item !== socket)
    if (sockets.length > 0) subscriptions.set(key, sockets)
    else subscriptions.delete(key)
  }

  const emit = (event, ...args) => {
    if (!socket.ref) return
    const key = getKey()
    const sockets = subscriptions.get(key) || []
    for (const sibling of sockets) {
      if (sibling === socket) continue
      sibling.emit(event, ...args)
    }
  }

  const getDocPath = () => {
    const { bucket, ref } = socket
    return path.join(storageDir, bucket, ref.substring(0, 2), `${ref}.json`)
  }

  const readDoc = async () => {
    const docPath = getDocPath()
    const docBackupPath = `${docPath}.backup`
    if (await fs.pathExists(docBackupPath)) {
      // Restore document from backup if needed
      fs.move(docBackupPath, docPath, { overwrite: true })
    }
    // Read document if it exists
    return (await fs.pathExists(docPath) &&
            await fs.readJson(docPath, { throws: false })) || undefined
  }

  const writeDoc = async (value) => {
    const docPath = getDocPath()
    const docBackupPath = `${docPath}.backup`
    if (await fs.pathExists(docPath)) {
      // Backup existing version of the document
      await fs.move(docPath, docBackupPath, { overwrite: true })
    }
    // Write document and remove backup
    await fs.outputJson(docPath, value)
    await fs.remove(docBackupPath)
  }

  socket.on('disconnect', (reason) => {
    unsubscribe()
    console.log(`Socket ${socket.id} disconnected due to ${reason}`)
  })

  socket.on('now', async (ack) => {
    if (typeof ack !== 'function') return socket.disconnect()
    const release = await execLock.readLock()
    try {
      ack({ timestamp: Date.now() })
    } catch (err) {
      return ack({ error: err.message })
    } finally {
      release()
    }
  })

  socket.on('ref', async (ref, ack) => {
    if (typeof ack !== 'function') return socket.disconnect()
    const release = await execLock.readLock()
    try {
      await refSchema.validateAsync(ref)
      subscribe(ref)
      ack()
    } catch (err) {
      return ack({ error: err.message })
    } finally {
      release()
    }
  })

  socket.on('get', async ({ known }, ack) => {
    if (typeof ack !== 'function') return socket.disconnect()
    const release = await execLock.readLock()
    try {
      assertRef()
      await versionSchema.validateAsync(known)
      docLock.acquire(getKey(), async (done) => {
        try {
          const doc = await readDoc()
          done(null, doc && {
            ...(doc.version !== known) && { data: doc.data },
            version: doc.version
          })
        } catch (err) {
          done(err)
        }
      }, (err, res) => {
        ack(err ? { error: err.message } : res)
      })
    } catch (err) {
      return ack({ error: err.message })
    } finally {
      release()
    }
  })

  socket.on('set', async ({ data, version }, ack) => {
    if (typeof ack !== 'function') return socket.disconnect()
    const release = await execLock.readLock()
    try {
      assertRef()
      await dataSchema.validateAsync(data)
      await versionSchema.validateAsync(version)
      docLock.acquire(getKey(), async (done) => {
        try {
          const nextDoc = { data }
          const prevDoc = await readDoc()
          if (prevDoc) {
            if ((prevDoc.version !== version)) {
              return done(null, {
                success: false,
                data: prevDoc.data,
                version: prevDoc.version
              })
            }
            nextDoc.version = version + 1
            nextDoc.created = prevDoc.created
            nextDoc.updated = Date.now()
          } else {
            nextDoc.version = 1
            nextDoc.created = Date.now()
          }
          nextDoc.ip = socket.ip || 'unknown'
          await writeDoc(nextDoc)
          done(null, {
            success: true,
            version: nextDoc.version
          })
          emit('changed')
        } catch (err) {
          done(err)
        }
      }, (err, res) => {
        ack(err ? { error: err.message } : res)
      })
    } catch (err) {
      return ack({ error: err.message })
    } finally {
      release()
    }
  })
})

const shutdown = async () => {
  await execLock.writeLock()
  console.log('\nShutting down...')
  server.close(() => {
    process.exit(0)
  })
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

server.listen(process.env.PORT || 3000)
