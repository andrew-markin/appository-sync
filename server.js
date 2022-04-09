import { fileURLToPath } from 'url'
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

const getDocPath = (ref) => {
  return path.join(storageDir, ref.substring(0, 2), `${ref}.json`)
}

const readDoc = async (ref) => {
  const docPath = getDocPath(ref)
  const docBackupPath = `${docPath}.backup`
  if (await fs.pathExists(docBackupPath)) {
    // Restore document from backup if needed
    fs.move(docBackupPath, docPath, { overwrite: true })
  }
  // Read document if it exists
  return (await fs.pathExists(docPath) &&
          await fs.readJson(docPath, { throws: false })) || undefined
}

const writeDoc = async (ref, value) => {
  const docPath = getDocPath(ref)
  const docBackupPath = `${docPath}.backup`
  if (await fs.pathExists(docPath)) {
    // Backup existing version of the document
    await fs.move(docPath, docBackupPath, { overwrite: true })
  }
  // Write document and remove backup
  await fs.outputJson(docPath, value)
  await fs.remove(docBackupPath)
}

// API routines

const server = new Server({ cors: { origin: true } })

server.use((socket, next) => {
  if (socket.handshake.auth.key === process.env.API_KEY) next()
  else next(new Error('Access denied'))
})

const subscriptions = new Map()

const assertAck = (ack) => {
  if (typeof ack !== 'function') {
    throw new Error('No acknowledgement callback provided')
  }
}

const assertRef = (socket) => {
  if (!socket.ref) {
    throw new Error('Reference is not provided')
  }
}

const refSchema = Joi.string().allow(null).lowercase().hex().length(64)
const dataSchema = Joi.string().base64().max(1024 * 1024)
const versionSchema = Joi.number().optional().min(1)

server.on('connection', (socket) => {
  console.log(`Socket ${socket.id} connected`)

  const unsubscribe = () => {
    const ref = socket.ref
    if (!ref) return
    let sockets = subscriptions.get(ref) || []
    sockets = sockets.filter((item) => item !== socket)
    if (sockets.length > 0) subscriptions.set(ref, sockets)
    else subscriptions.delete(ref)
  }

  const subscribe = (ref) => {
    unsubscribe()
    socket.ref = ref
    if (!ref) return
    const sockets = subscriptions.get(ref) || []
    sockets.push(socket)
    subscriptions.set(ref, sockets)
  }

  const emit = (event, ...args) => {
    const ref = socket.ref
    if (!ref) return
    const sockets = subscriptions.get(ref) || []
    for (const sibling of sockets) {
      if (sibling === socket) continue
      sibling.emit(event, ...args)
    }
  }

  socket.on('disconnect', (reason) => {
    unsubscribe()
    console.log(`Socket ${socket.id} disconnected due to ${reason}`)
  })

  socket.on('now', async (ack) => {
    const release = await execLock.readLock()
    try {
      assertAck(ack)
      ack({ timestamp: Date.now() })
    } catch (err) {
      return ack({ error: err.message })
    } finally {
      release()
    }
  })

  socket.on('ref', async (ref, ack) => {
    const release = await execLock.readLock()
    try {
      assertAck(ack)
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
    const release = await execLock.readLock()
    try {
      assertAck(ack)
      assertRef(socket)
      await versionSchema.validateAsync(known)
      const ref = socket.ref
      docLock.acquire(ref, async (done) => {
        try {
          const doc = await readDoc(ref)
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
    const release = await execLock.readLock()
    try {
      assertAck(ack)
      assertRef(socket)
      await dataSchema.validateAsync(data)
      await versionSchema.validateAsync(version)
      const ref = socket.ref
      docLock.acquire(ref, async (done) => {
        try {
          const nextDoc = { data }
          const prevDoc = await readDoc(ref)
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
          await writeDoc(ref, nextDoc)
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

server.listen(3000)
