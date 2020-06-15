const mutexify = require('mutexify/promise')
const { Entry } = require('./messages')

class Batch {
  constructor (seq, feed, head) {
    this.seq = seq
    this.feed = feed
    this.head = Entry.decode(head)
    this.length = this.head.blockIndex + 1
    this.index = this.head.batchIndex
  }

  get (index) {
    return new Promise((resolve, reject) => {
      if (index >= this.length || index < 0) return reject(new Error('Out of bounds'))
      if (index === this.length - 1) return resolve(this.head.block)
      const seq = this.seq - this.length + 1 + index
      this.feed.get(seq, (err, data) => {
        if (err) return reject(err)
        resolve(Entry.decode(data).block)
      })
    })
  }
}

module.exports = class HyperBatch {
  constructor (feed) {
    this.feed = feed
    this.lock = mutexify()
  }

  async append (blocks) {
    const release = await this.lock()

    try {
      const head = await this.head()
      const batchIndex = head ? head.index + 1 : 0

      let byteLength = head ? head.head.byteLength : 0
      let seq = head ? head.seq + 1 : 0
      const entries = new Array(blocks.length)

      for (let i = 0; i < blocks.length; i++) {
        const block = blocks[i]
        const index = i === blocks.length - 1 ? [] : null

        if (index) {
          let f = 1
          while (true) {
            const t = batchIndex - f
            if (t < 0) break
            f *= 2
            const { seq } = await this.get(t)
            index.push(seq)
          }
        }

        seq++
        entries[i] = Entry.encode({
          batchIndex,
          blockIndex: i,
          byteLength: byteLength += block.length,
          index: compress(seq - 1, index),
          block
        })
      }

      await this._append(entries)
      return batchIndex
    } finally {
      release()
    }
  }

  async length () {
    const head = await this.head()
    return head ? head.index + 1 : 0
  }

  async byteLength () {
    const head = await this.head()
    return head ? head.head.byteLength : 0
  }

  async get (batchIndex) {
    let batch = await this.head()

    while (batch) {
      if (batch.index === batchIndex) return batch
      if (batch.index < batchIndex) break

      batch = await this._gallop(batch, batchIndex)
    }

    throw new Error('Cannot find batch')
  }

  getBySeq (seq) {
    return new Promise((resolve, reject) => {
      this.feed.get(seq, (err, data) => {
        if (err) reject(err)
        else resolve(new Batch(seq, this.feed, data))
      })
    })
  }

  async head () {
    await new Promise((resolve) => {
      this.feed.update({ ifAvailable: true }, () => resolve())
    })

    const length = await new Promise((resolve, reject) => {
      this.feed.ready((err) => {
        if (err) return reject(err)
        resolve(this.feed.length)
      })
    })

    return length ? this.getBySeq(length - 1) : null
  }

  _append (blocks) {
    return new Promise((resolve, reject) => {
      this.feed.append(blocks, err => {
        if (err) reject(err)
        else resolve()
      })
    })
  }

  _getBySeqSafe (seq, prev) {
    if (!(seq < prev)) return null
    return this.getBySeq(seq)
  }

  _gallop (batch, target) {
    let f = 1

    const index = decompress(batch.seq, batch.head.index)

    for (let i = 0; i < index.length; i++) {
      const t = batch.index - f
      f *= 2

      if (t === target) return this._getBySeqSafe(index[i], batch.seq)
      if (t < target) return this._getBySeqSafe(index[i - 1], batch.seq)
    }

    if (!index.length) return null
    return this._getBySeqSafe(index[index.length - 1], batch.seq)
  }
}

function compress (seq, arr) {
  if (!arr) return null

  const res = new Array(arr.length)
  for (let i = 0; i < arr.length; i++) {
    res[i] = seq - arr[i]
    seq = arr[i]
  }
  return res
}

function decompress (seq, arr) {
  const res = new Array(arr.length)
  for (let i = 0; i < arr.length; i++) {
    seq = res[i] = seq - arr[i]
  }
  return res
}
