'use strict'

const mkdir = require('mkdir-p')
const fs = require('fs')
const os = require('os')
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
      lastPersistDataTimeout: null
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
      log('Database exists, reading database into memory...')
      let readlines = require('n-readlines');
      let startTime = Date.now()
      let tableNames = fs.readdirSync(this.paths.tabledir(''))
      for (let i = 0; i < tableNames.length; i++) {
        let tableName = tableNames[i]
        this.createTable(tableName)
        let liner = new readlines(this.paths.indexfile(tableName));
        let lastId = parseInt(liner.next())
        let items = []
        let fd = fs.openSync(this.paths.tablefile(tableName), 'r');
        // Prevents unnecessary new buffer allocations up until that size.
        // I think 1 KiB of buffer should be enough for average items
        // and is OK to use since we might get a performance boost later.
        // It will be GC-collected later, anyway.
        let buf = new Buffer(1024)
        let lines = 0
        let length;
        while (length = parseInt(liner.next().toString())) {
          if (buf.length > length) {
            buf.writeInt8(0, length, true)
          } else if (buf.length < length) {
            // It's going to be filled completely anyway
            buf = Buffer.allocUnsafe(length);
          }
          items.push(JSON.parse(buf.toString('utf8', 0,
            fs.readSync(fd, buf, 0, length))))
          lines++
        }
        log(`Table ${tableName} loaded, ${lines} items`)
        fs.closeSync(fd);
        this.storage.tables[tableName].lastId = lastId
        this.storage.tables[tableName].items = items
      }
      log(`Tables loaded: ${Object.keys(this.storage.tables).length}, ` +
            (Date.now() - startTime) + ' ms')
    } else {
      log(`New database, will be created once data is persisted`)
    }
  }

  createTable (table) {
    this.storage.tables[table] = {
      lastId: 0,
      items: []
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
    let normalRunDue = this.storage.writes >= 100 &&
         this.storage.lastWrite + 1000 * 60 * this.storage.writes / 100 <=
          dateNow
    let longRunDue = this.storage.writes >= 1000 &&
                      this.storage.lastWrite + 1000 <= dateNow
    let shortRunDue = this.storage.writes < 100 &&
       this.storage.lastWrite + 1000 * 60 * 2 <= dateNow
    if (normalRunDue || longRunDue || shortRunDue) {
      (async () => {
        this.persistData().then(() => {}, e => throwAndLog(e))
                          .catch(e => throwAndLog(e))
      })()
    } else {
      if (this.lastPersistDataTimeout != null) {
        clearTimeout(this.lastPersistDataTimeout)
      }
      this.lastPersistDataTimeout = setTimeout(() => {
        this.lastPersistDataTimeout = null
        this.asyncPersistData()
      }, (this.storage.writes > 1000 ? 1 : 60) * 1000)
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
        let file = appendFile(this.paths.tablefile(tableName), 'w')
        let indexfile = appendFile(this.paths.indexfile(tableName), 'w')
        let table = this.storage.tables[tableName]
        indexfile.append(`${table.lastId}`)
        for (let rowId in table.items) {
          let data = JSON.stringify(table.items[rowId])
          file.append(data)
          indexfile.append('\n')
          indexfile.append(`${data.length}`)
          if (rowId % 100000 === 0) {
            if (os.freemem() < this.storage.leastfree) {
              reject(new Error(
                `Less than ${this.storage.leastfreemib} MiB system memory ` +
                `available: ${os.freemem() / 1024 / 1024} MiB.` +
                "You're on your own :/"))
              return
            }
          }
        }
        file.close()
        indexfile.close()
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
    tbl.items.push(data)
    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.asyncPersistData()
    return tbl.lastId
  }

  update (table, data) {
    if (data.id === undefined || data.id === 0) {
      throw Error('id must exist and be at least 1')
    }
    this.storage.tables[table].items[
      this.storage.tables[table].items.findIndex((item) => {
        return item.id === data.id
      })
    ] = data
    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.asyncPersistData()
    return this.get(table, data.id)
  }

  remove (table, id) {
    if (id === undefined || id === 0) {
      throw Error('id must exist and be at least 1')
    }
    delete this.storage.tables[table].items[
      this.storage.tables[table].findIndex((item) => {
        return item.id === id
      })
    ]
    if (id === this.storage.tables[table].lastId) {
      this.storage.tables[table].lastId--
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
    let length = this.storage.tables[table].items.length =
      index != -1 ? index : 0

    this.storage.lastWrite = Date.now()
    this.storage.writes++
    this.storage.tables[table].lastId = start
    this.asyncPersistData()
    return length
  }

  get (table, id) {
    if (id === undefined || id === 0) {
      throw Error('id must exist and be at least 1')
    }
    return this.storage.tables[table].items.find((item) => {
      return item.id === id
    })
  }

  getAll (table) {
    return this.storage.tables[table].items
  }
}

module.exports = (dir = 'datastorage') => {
  return new Database(dir)
}
