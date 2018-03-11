'use strict'

const mkdir = require('mkdir-p')
const fs = require('fs')
const os = require('os')
const msgpack = require('msgpack')
const readlines = require('n-readlines')
var util = null

function throwAndLog(msg) {
  console.error(msg)
  throw Error(msg)
}

function appendFile(file, mode = 'a') {
  let stream = fs.createWriteStream(file, { flags: mode })
  return {
    append (content) {
      stream.write(content)
    },
    async close () {
      stream.end()
    }
  }
}

function log(msg) {
  console.log(`[ezpzdb] ${msg}`)
  return msg
}

function logi(obj) {
  if (util === null) util = require('util')
  return log(util.inspect(obj))
}

function readFilePartialSync (filename, start, length) {
  var buf = new Buffer(length)
  var fd = fs.openSync(filename, 'r')
  fs.readSync(fd, buf, 0, length, start)
  fs.closeSync(fd)
  return buf
}

class Database {

  constructor (dir) {
    let thiz = this
    this.storage = {
      tables: {

      },
      dir,
      exists: false,
      writes: 0,
      lastPersistDataTimeout: null,
      worthPersistingNow: false
    }
    this.paths = {
      tabledir (table) {
        return `${thiz.storage.dir}/tables/${table}`
      },
      indexfile (table) {
        return `${thiz.paths.tabledir(table)}/index`
      },
      tablefile (table) {
        return `${thiz.paths.tabledir(table)}/db`
      }
    }
    this.initialize()
  }

  initialize() {
    log('Easy Peasy Lemon Squeezy Databasey')
    log('Here to serve.')
    log('')
    log('Initializing...')
    mkdir.sync(this.storage.dir)
    // At least the percentage below of memory has to be available
    // Has to be at least 64 MiB and no more than 1 GiB
    let leastfree = Math.floor(os.totalmem() * 0.06)
    leastfree = Math.max(64 * 1024 * 1024, leastfree)
    leastfree = Math.min(1024 * 1024 * 1024, leastfree)
    let leastfreemib = Math.floor(leastfree / 1024 / 1024)
    log(`Watching your memory usage - I need ${leastfreemib} MiB available`)
    this.storage.leastfree = leastfree
    this.storage.leastfreemib = leastfreemib
    // This file is used to indicate that the
    // database has already been created and is ready to use
    this.paths.stamp = `${this.storage.dir}/.stamp`
    if (this.storage.exists = fs.existsSync(this.paths.stamp)) {
      log('Database exists, reading information...')
      let tableNames = fs.readdirSync(this.paths.tabledir(''))
      for (let i = 0; i < tableNames.length; i++) {
        let tableName = tableNames[i]
        this.createTable(tableName)
        let liner = new readlines(this.paths.indexfile(tableName));
        let next = liner.next()
        let lastId = 0
        if (next) lastId = parseInt(next)
        // Asynchronously get to the end of the file
        ;(async () => { while (liner.next()); })()
        this.storage.tables[tableName].lastId = lastId
        log(`Table ${tableName} initialized, last id: ${lastId}`)
      }
    } else {
      log(`New database, will be created once data is persisted`)
    }
  }

  createTable (table) {
    this.storage.tables[table] = {
      // Last inserted ID. Can only increment, except if truncated
      lastId: 0,
      items: [],
      // Items to insert when persisting, full item
      inserts: [],
      // Items to update when persisting, full item
      updates: [],
      // Items to remove when persisting, just id
      removals: [],
      // >= 0 means truncate to that
      truncate: -1,
      // Cache
      cache: []
    }
  }

  /**
   * Finds the best time to persist data and schedules it accordingly
   */
  asyncPersistData() {
    /*
     * Data is persisted when:
     *  - more than or exactly 100 writes have occured AND at
     *    least (writes / 100) minutes have passed since the last write
     *  - at least 1000 writes occured AND at least
     *    one second passed since the last write
     *  - Less than 100 writes have occured AND at least 2 minutes passed
     *    since the last write
     *
     * This is to ensure performance when writing batches while maintaining
     * reliable data persistance.
     *
     * 1 write is inserting/updating/removing once
     */
    let dateNow = Date.now()
    let writes = this.storage.writes
    let lastWrite = this.storage.lastWrite
    let normalRunDue =
      writes >= 100 && lastWrite + 1000 * 60 * writes / 100 <= dateNow
    let longRunDue = writes >= 1000 && lastWrite + 1000 <= dateNow
    let shortRunDue = writes < 100 && lastWrite + 1000 * 60 * 2 <= dateNow
    if (normalRunDue || longRunDue || shortRunDue ||
        this.storage.worthPersistingNow) {
      (async () => {
        this.persistData().then(() => {}, e => throwAndLog(e))
                          .catch(e => throwAndLog(e))
      })()
      this.storage.worthPersistingNow = false
    } else {
      if (this.lastPersistDataTimeout != null) {
        clearTimeout(this.lastPersistDataTimeout)
      }
      this.lastPersistDataTimeout = setTimeout(() => {
        this.lastPersistDataTimeout = null
        this.asyncPersistData()
      }, (writes > 1000 ? 1 : 60) * 1000)
    }
  }

  persistData () {
    return new Promise((resolve, reject) => {
      if (!this.storage.exists) {
        log("Now it's time to create a new database")
        fs.writeFile(this.paths.stamp, `${Date.now()}`, err => {
          if (err) reject(err)
        })
        this.storage.exists = true
      }
      global.gc()
      log("Persisting data...")
      for (let tableName in this.storage.tables) {
        mkdir.sync(this.paths.tabledir(tableName))
        if (table.truncate >= 0) {
          if (table.truncate === 0) {
            // Quick-truncate since there are no items left
            fs.truncateSync(this.paths.indexfile(tableName), 0)
            fs.writeFileSync(this.paths.indexfile(tableName), '0')
            fs.truncateSync(this.paths.tablefile(tableName), 0)
            // And without items there are no removals
            this.storage.tables[tableName].removals.length = 0
          } else {
            // This is the position the table file is going to be truncated at
            let tableTruncPos = 0
            // We need to go through the index to find the correct
            // truncate position as well as the position for
            // the table file.
            let liner = new readlines(this.paths.indexfile(tableName))
            // This is the position the index file is going to be truncated at
            let truncPos = liner.next().length
            let next
            while (next = liner.next()) {
              let split = parseInt(next.toString().split(',')
              if (split[1]) <= table.truncate) {
                truncPos += next.length
                tableTruncPos += parseInt(split[0])
              } else {
                // Finish it up
                ;(async () => { while (liner.next()); })()
                break
              }
            }
            fs.truncate(this.paths.indexfile(tableName), truncPos)
            fs.truncate(this.paths.tablefile(tableName), tableTruncPos)
          }
        }
        table.removals.sort((a, b) => {
          if (a.id > b.id) return 1
          if (a.id < b.id) return -1
          return 0
        })
        for (let i = 0; i < table.removals.length; i++) {
          let removal = table.removals[i]
          let fd = fs.openSync(this.paths.indexfile(tableName), 'r')
          let tfd = fs.openSync(this.paths.tablefile(tableName), 'r')
          let nifd = fs.openSync(this.paths.indexfile(tableName) + '.new', 'w')
          let ntfd = fs.openSync(this.paths.tablefile(tableName) + '.new', 'w')
          let nifile = appendFile(nifd)
          let ntfile = appendFile(ntfd)
          let liner = new readlines(fd)
          let next
          let pos = 0
          while (next = liner.next()) {
            let split = next.toString().split(',')
            split = [parseInt(split[0]), parseInt(split[1])]
            if (!(split[1] === removal && removal <= table.lastId)) {
              nifile.append('\n')
              nifile.append(next)
              let buf = new Buffer(split[0])
              ntfile.append(fs.readSync(fd, buf, 0, split[0], pos))
            }
            pos += split[0]
          }
          fs.closeSync(fd)
          fs.closeSync(tfd)
          nifile.close()
          ntfile.close()
        }
        for (let i = 0; i < table.updates.length; i++) {
          let update = table.updates[i]
          let fd = fs.openSync(this.paths.indexfile(tableName), 'r')
          let tfd = fs.openSync(this.paths.tablefile(tableName), 'r')
          let nifd = fs.openSync(this.paths.indexfile(tableName) + '.new', 'w')
          let ntfd = fs.openSync(this.paths.tablefile(tableName) + '.new', 'w')
          let nifile = appendFile(nifd)
          let ntfile = appendFile(ntfd)
          let liner = new readlines(fd)
          let next
          let pos = 0
          while (next = liner.next()) {
            let split = next.toString().split(',')
            split = [parseInt(split[0]), parseInt(split[1])]
            nifile.append('\n')
            if (split[1] !== update.id) {
              let buf = new Buffer(split[0])
              ntfile.append(fs.readSync(fd, buf, 0, split[0], pos))
              nifile.append(next)
            } else {
              let data
              ntfile.append(data = msgpack.pack(update))
              nifile.append(`${data.length},${update.id}`)
            }
            pos += split[0]
          }
          fs.closeSync(fd)
          fs.closeSync(tfd)
          nifile.close()
          ntfile.close()
        }
        this.storage.writes = 0
        global.gc()
      }
      log("...done")
    })
  }

  insert (table, data) {
    if ((this.storage.writes + 1) % 250000 === 0) {
      if (os.freemem() < this.storage.leastfree) {
        if (this.lastPersistDataTimeout != null) {
          clearTimeout(this.lastPersistDataTimeout)
        }
        let error =
          `Less than ${this.storage.leastfreemib} MiB system memory ` +
          `available: ${os.freemem() / 1024 / 1024} MiB. ` +
          "You're on your own :/"
        console.log(error)
        throw new Error(error)
        return null
      } else if (this.storage.writes % 2000000 === 0) global.gc()
    }
    if (!this.storage.tables[table]) {
      this.createTable(table)
    }
    let tbl = this.storage.tables[table]
    tbl.beforeLastId = tbl.lastId
    data.id = ++tbl.lastId
    tbl.inserts.push(data)
    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.asyncPersistData()
    return tbl.lastId
  }

  update (table, data) {
    if (data.id === undefined || data.id === 0) {
      throw Error('id must exist and be at least 1')
    }
    let insertIndex = this.storage.tables[table].inserts.findIndex((item) => {
      return item.id === data.id
    })
    if (insertIndex !== -1) {
      this.storage.tables[table].inserts[insertIndex] = data
    } else {
      this.storage.tables[table].updates.push(data)
    }

    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.asyncPersistData()
    return this.get(table, data.id)
  }

  remove (table, id) {
    if (id === undefined || id === 0) {
      throw Error('id must exist and be at least 1')
    }
    let insertIndex = this.storage.tables[table].inserts.findIndex((item) => {
      return item.id === id
    })
    if (insertIndex !== -1) {
      this.storage.tables[table].inserts.splice(insertIndex, 1)
    } else {
      this.storage.tables[table].removals.push(id)
    }
    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.asyncPersistData()
    return true
  }

  /**
   * table: Table to truncate
   * start: Anything bigger than this will be gone
   */
  truncate (table, start = 0) {
    let index = this.storage.tables[table].items.findIndex((item) => {
      return item.id > start
    })

    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.storage.tables[table].lastId = start
    this.storage.worthPersistingNow = true
    this.storage.tables[table].truncate = start
    this.asyncPersistData()
    return index
  }

  get (table, id) {
    if (id === undefined || id === 0) {
      throw Error('id must exist and be at least 1')
    }
    let item = this.storage.tables[table].updates.find((item) => {
      return item.id === id
    })
    if (!item) {
      item = this.storage.tables[table].inserts.find((item) => {
        return item.id === id
      })
    }
    if (!item) {
      if (this.storage.tables[table].removals.find((item) => {
        return item.id === id
      })) return null
      item = this.storage.tables[table].cache.find((item) => {
        return item.id === id
      })
      if (!item) {
        let tableName = table
        let liner = new readlines(this.paths.indexfile(tableName))
        liner.next()
        let fd = fs.openSync(this.paths.tablefile(tableName), 'r')
        // Prevents unnecessary new buffer allocations up until that size.
        // I think 1 KiB of buffer should be enough for average items
        // and is OK to use since we might get a performance boost later.
        // It will be GC-collected later, anyway.
        let buf = new Buffer(1024)
        let length
        while (length = parseInt(liner.next().toString())) {
          if (buf.length > length) {
            buf.writeInt8(0, length, true)
          } else if (buf.length < length) {
            // It's going to be filled completely anyway
            buf = Buffer.allocUnsafe(length);
          }
          item = msgpack.unpack(buf)
          if (item.id === id) {
            break
          } else item = null
        }
        fs.closeSync(fd)
      }
      return item
    }
  }

  getAll (table, until = 0) {
    let items = []
    let tableName = table
    let liner = new readlines(this.paths.indexfile(tableName))
    liner.next()
    let fd = fs.openSync(this.paths.tablefile(tableName), 'r')
    // Prevents unnecessary new buffer allocations up until that size.
    // I think 1 KiB of buffer should be enough for average items
    // and is OK to use since we might get a performance boost later.
    // It will be GC-collected later, anyway.
    let buf = new Buffer(1024)
    let length
    while (length = parseInt(liner.next().toString())) {
      if (buf.length > length) {
        buf.writeInt8(0, length, true)
      } else if (buf.length < length) {
        // It's going to be filled completely anyway
        buf = Buffer.allocUnsafe(length);
      }
      let item
      items.push(item = msgpack.unpack(buf))
      if (item.id === until) break
    }
    fs.closeSync(fd)
    return items
  }
}

module.exports = (dir = 'datastorage') => {
  return new Database(dir)
}
