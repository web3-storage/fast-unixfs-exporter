import extractDataFromBlock from '../../../utils/extract-data-from-block.js'
import validateOffsetAndLength from '../../../utils/validate-offset-and-length.js'
import { UnixFS } from 'ipfs-unixfs'
import errCode from 'err-code'
import * as dagPb from '@ipld/dag-pb'
import * as dagCbor from '@ipld/dag-cbor'
import * as raw from 'multiformats/codecs/raw'

/**
 * @typedef {import('../../../types').ExporterOptions} ExporterOptions
 * @typedef {import('interface-blockstore').Blockstore} Blockstore
 * @typedef {import('@ipld/dag-pb').PBNode} PBNode
 *
 * @param {Blockstore} blockstore
 * @param {PBNode} node
 * @param {number} start
 * @param {number} end
 * @param {number} streamPosition
 * @param {ExporterOptions} options
 * @returns {AsyncIterable<Uint8Array>}
 */
async function * emitBytes (blockstore, node, start, end, streamPosition = 0, options) {
  // a `raw` node
  if (node instanceof Uint8Array) {
    const buf = extractDataFromBlock(node, streamPosition, start, end)

    if (buf.length) {
      yield buf
    }

    streamPosition += buf.length

    return streamPosition
  }

  if (node.Data == null) {
    throw errCode(new Error('no data in PBNode'), 'ERR_NOT_UNIXFS')
  }

  let file

  try {
    file = UnixFS.unmarshal(node.Data)
  } catch (/** @type {any} */ err) {
    throw errCode(err, 'ERR_NOT_UNIXFS')
  }

  // might be a unixfs `raw` node or have data on intermediate nodes
  if (file.data && file.data.length) {
    const buf = extractDataFromBlock(file.data, streamPosition, start, end)

    if (buf.length) {
      yield buf
    }

    streamPosition += file.data.length
  }

  let childStart = streamPosition

  // work out which child nodes contain the requested data
  for (let i = 0; i < node.Links.length; i++) {
    const childLink = node.Links[i]
    const childEnd = streamPosition + file.blockSizes[i]

    if ((start >= childStart && start < childEnd) || // child has offset byte
        (end > childStart && end <= childEnd) || // child has end byte
        (start < childStart && end > childEnd)) { // child is between offset and end bytes
      const block = await blockstore.get(childLink.Hash, {
        signal: options.signal
      })
      let child
      switch (childLink.Hash.code) {
        case dagPb.code:
          child = dagPb.decode(block)
          break
        case raw.code:
          child = block
          break
        case dagCbor.code:
          child = dagCbor.decode(block)
          break
        default:
          throw Error(`Unsupported codec: ${childLink.Hash.code}`)
      }

      for await (const buf of emitBytes(blockstore, child, start, end, streamPosition, options)) {
        streamPosition += buf.length

        yield buf
      }
    }

    streamPosition = childEnd
    childStart = childEnd + 1
  }
}

/**
 * @param {Blockstore} blockstore
 * @param {PBNode} node
 * @param {number} streamPosition
 * @param {ExporterOptions} options
 * @returns {AsyncIterable<Uint8Array>}
 */
async function * emitAllBytes (blockstore, node, streamPosition = 0, options) {
  if (node instanceof Uint8Array) {
    const buf = extractDataFromBlock(node, streamPosition, streamPosition, streamPosition + node.length)
    if (buf.length) {
      yield buf
    }
    streamPosition += buf.length
    return streamPosition
  }

  if (node.Data == null) {
    throw errCode(new Error('no data in PBNode'), 'ERR_NOT_UNIXFS')
  }

  let file
  try {
    file = UnixFS.unmarshal(node.Data)
  } catch (err) {
    throw errCode(err, 'ERR_NOT_UNIXFS')
  }

  if (file.data && file.data.length) {
    const buf = extractDataFromBlock(file.data, streamPosition, streamPosition, streamPosition + file.data.length)
    if (buf.length) {
      yield buf
    }
    streamPosition += file.data.length
  }

  const blocks = (async function * () {
    const blockPromises = node.Links.map(l => (
      blockstore.get(l.Hash, { signal: options.signal })
        .then(block => ({ block }))
        .catch(error => ({ error }))
    ))
    while (true) {
      const promise = blockPromises.shift()
      if (!promise) return
      const res = await promise
      if ('error' in res) throw res.error
      yield res.block
    }
  })()

  let i = 0
  for await (const block of blocks) {
    const childLink = node.Links[i]
    const childEnd = streamPosition + file.blockSizes[i]

    let child
    switch (childLink.Hash.code) {
      case dagPb.code:
        child = dagPb.decode(block)
        break
      case raw.code:
        child = block
        break
      case dagCbor.code:
        child = dagCbor.decode(block)
        break
      default:
        throw Error(`Unsupported codec: ${childLink.Hash.code}`)
    }
    for await (const buf of emitAllBytes(blockstore, child, streamPosition, options)) {
      streamPosition += buf.length
      yield buf
    }
    streamPosition = childEnd
    i++
  }
}

/**
 * @type {import('../').UnixfsV1Resolver}
 */
const fileContent = (cid, node, unixfs, path, resolve, depth, blockstore) => {
  /**
   * @param {ExporterOptions} options
   */
  function yieldFileContent (options = {}) {
    const fileSize = unixfs.fileSize()

    if (fileSize === undefined) {
      throw new Error('File was a directory')
    }

    const {
      offset,
      length
    } = validateOffsetAndLength(fileSize, options.offset, options.length)

    if (offset === 0 && length === fileSize) {
      return emitAllBytes(blockstore, node, 0, options)
    } else {
      const start = offset
      const end = offset + length
      return emitBytes(blockstore, node, start, end, 0, options)
    }
  }

  return yieldFileContent
}

export default fileContent
