# hyperbatch

Addressable batches of data on top of a [Hypercore](https://github.com/mafintosh/hypercore)

```
npm install hyperbatch
```

## Usage

``` js
const Hyperbatch = require('hyperbatch')

const b = new Hyperbatch(feed) // pass a hypercore

// append some batches
await b.append([
  Buffer.from('block #0'),
  Buffer.from('block #1')
])

await b.append([
  Buffer.from('block #2'),
  Buffer.from('block #3'),
  Buffer.from('block #4')
])

await b.append([
  Buffer.from('block #5')
])

// how many batches are appended?
await b.length()

// how much data in all batches?
await b.byteLength()

// get a batch
const batch = await b.get(1) // get the second batch

console.log(batch.length) // 3 entries
const block = await batch.get(0) // 'block #2'
const block = await batch.get(1) // 'block #3'
```

## License

MIT
