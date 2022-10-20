/**
 * @typedef {import('../../../types').ExporterOptions} ExporterOptions
 * @typedef {import('../../../types').UnixfsV1DirectoryContent} UnixfsV1DirectoryContent
 * @typedef {import('../../../types').UnixfsV1Resolver} UnixfsV1Resolver
 */

/**
 * @type {UnixfsV1Resolver}
 */
const directoryContent = (cid, node, unixfs, path, resolve, depth, blockstore) => {
  /**
   * @param {ExporterOptions} [options]
   * @returns {UnixfsV1DirectoryContent}
   */
  async function * yieldDirectoryContent (options = {}) {
    const offset = options.offset || 0
    const length = options.length || node.Links.length
    const links = node.Links.slice(offset, length)

    const results = (async function * () {
      const resultPromises = links.map(l => (
        resolve(l.Hash, l.Name || '', `${path}/${l.Name || ''}`, [], depth + 1, blockstore, options)
          .then(result => ({ result }))
          .catch(error => ({ error }))
      ))
      while (true) {
        const promise = resultPromises.shift()
        if (!promise) return
        const res = await promise
        if ('error' in res) throw res.error
        yield res.result
      }
    })()

    for await (const result of results) {
      if (result.entry) {
        yield result.entry
      }
    }
  }

  return yieldDirectoryContent
}

export default directoryContent
