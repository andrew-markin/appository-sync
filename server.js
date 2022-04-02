const { Server } = require('socket.io')
const AsyncLock = require('async-lock')
const fs = require('fs-extra')
const Joi = require('joi')
const path = require('path')

const io = new Server({ cors: { origin: true } })
const subscriptions = new Map()

// Storage routines

const docLock = new AsyncLock()

const storage = process.env.STORAGE || path.join(__dirname, './storage')
fs.ensureDirSync(storage)

const getDocPath = (ref) => {
  return path.join(storage, ref.substring(0, 2), `${ref}.json`)
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

io.on('connection', (socket) => {
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

  socket.on('now', (ack) => {
    try {
      assertAck(ack)
      ack({ timestamp: Date.now() })
    } catch (err) {
      return ack({ error: err.message })
    }
  })

  socket.on('ref', async (ref, ack) => {
    try {
      assertAck(ack)
      await refSchema.validateAsync(ref)
      subscribe(ref)
      ack()
    } catch (err) {
      return ack({ error: err.message })
    }
  })

  socket.on('get', async ({ known }, ack) => {
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
    }
  })

  socket.on('set', async ({ data, version }, ack) => {
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
    }
  })
})

io.listen(3000)
