import { decode } from '@ipld/dag-pb'

/**
 * @typedef {import('interface-blockstore').Blockstore} Blockstore
 * @typedef {import('../../../types').ExporterOptions} ExporterOptions
 * @typedef {import('../../../types').Resolve} Resolve
 * @typedef {import('../../../types').UnixfsV1DirectoryContent} UnixfsV1DirectoryContent
 * @typedef {import('../../../types').UnixfsV1Resolver} UnixfsV1Resolver
 * @typedef {import('@ipld/dag-pb').PBNode} PBNode
 */

/**
 * @type {UnixfsV1Resolver}
 */
const hamtShardedDirectoryContent = (cid, node, unixfs, path, resolve, depth, blockstore) => {
  /**
   * @param {ExporterOptions} options
   *
   */
  function yieldHamtDirectoryContent (options = {}) {
    return listDirectory(node, path, resolve, depth, blockstore, options)
  }

  return yieldHamtDirectoryContent
}

/**
 * @param {PBNode} node
 * @param {string} path
 * @param {Resolve} resolve
 * @param {number} depth
 * @param {Blockstore} blockstore
 * @param {ExporterOptions} options
 *
 * @returns {UnixfsV1DirectoryContent}
 */
async function * listDirectory (node, path, resolve, depth, blockstore, options) {
  const links = node.Links

  const results = (async function * () {
    const resultPromises = links.map(async l => {
      try {
        const name = l.Name != null ? l.Name.substring(2) : null
        if (name) {
          const result = await resolve(l.Hash, name, `${path}/${name}`, [], depth + 1, blockstore, options)
          return { entries: [result.entry] }
        } else {
          // descend into subshard
          const block = await blockstore.get(l.Hash)
          node = decode(block)
          return { entries: listDirectory(node, path, resolve, depth, blockstore, options) }
        }
      } catch (error) {
        return { error }
      }
    })
    for (const promise of resultPromises) {
      const res = await promise
      if ('error' in res) throw res.error
      yield res.entries
    }
  })()

  for await (const entries of results) {
    yield * entries
  }
}

export default hamtShardedDirectoryContent
