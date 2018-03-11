'use strict'

module.exports = class EffSet {

  constructor () {
    this.fields = []
    this.entries = []
  }

  push (elem) {
    let entry = new Array(this.fields.length)
    for (let key in elem) {
      if (!(key in this.fields)) {
        this.fields.push(key)
      }
      entry[this.fields.indexOf(key)] = elem[key]
    }
    this.entries.push(entry)
  }

  get length () {
    return this.entries.length
  }

  set length (value) {
    this.entries.length = value
  }

  get (index) {
    let entry = this.entries[index]
    if (!entry) {
      return undefined
    }
    let obj = {}
    for (let i = 0; i < this.fields.length; i++) {
      obj[this.fields[i]] = entry[i]
    }
    return obj
  }

  getAll () {
    let all = []
    for (let i = 0; i < this.entries.length; i++) {
      all.push(this.get(i))
    }
    return all
  }

  set (index, elem) {
    for (let key in elem) {
      if (!(key in this.fields)) {
        this.fields.push(key)
      }
      let entry = []
      entry[this.fields.indexOf(key)] = elem[key]
      this.entries[index] = entry
    }
  }

  remove (index) {
    this.entries[index] = null
  }

  findIndex (func) {
    for (let i = 0; i < this.entries.length; i++) {
      if (func(this.get(i))) {
        return i
      }
    }
  }

  find (func) {
    let item
    for (let i = 0; i < this.entries.length; i++) {
      if (func(item = this.get(i))) {
        return item
      }
    }
  }

}
