const S3Datastore = require('./index.js');
const NodeCache = require('node-cache');
const once = require('once');
const _ = require('lodash');
/* :: export type S3DSInputOptions = {
  s3: S3Instance,
  createIfMissing: ?boolean
}
declare type S3Instance = {
  config: {
    params: {
      Bucket: ?string
    }
  },
  deleteObject: any,
  getObject: any,
  headBucket: any,
  headObject: any,
  listObjectsV2: any,
  upload: any
}
*/

/**
 * A datastore backed by the file system.
 * Using an LRU cache
 * Keys need to be sanitized before use, as they are written
 * to the file system as is.
 */
class cachedS3Datastore extends S3Datastore {
  /* :: path: string */
  /* :: opts: S3DSInputOptions */
  /* :: bucket: string */
  /* :: createIfMissing: boolean */

  constructor (path , opts) {
    super(path, opts);
    this.opts = opts;
    this.path = path;
    this.cacheReady = false;
    //if this repo is handling blocks, get all keys with the /blocks prefix and add them to a cache
    //this caching prevents 100+ s3 requests per second at the cost of a slight delay in initialization
    if (_.includes(this.path, '/blocks')) {
      this._listInitialKeys( { Prefix: this.path.substring(1) },[], (err, keys) => {
        if (err) {
          throw err;
        }
        this.indexCache = new NodeCache({checkperiod: 0});
        _.map(keys , key => {
          this.indexCache.set(key, true);
        });
        this.cacheReady = true;
      });
    }
  }
  /**
   * Store the given value under the key.
   *
   * @param {Key} key
   * @param {Buffer} val
   * @param {function(Error)} callback
   * @returns {void}
   */

  _getFullKey(key ){
    return super._getFullKey(key);
  }
  
    /**
   * Put into s3 and the cache.
   *
   * @param {Key} key
   * @param {Buffer} val
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   */
  put (key /* : Key */, val /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    callback = once(callback);
    super.put(key, val, callback);
    if (this.cacheReady) {
      this.indexCache.set(key, true); 
    }
  }

  /**
   * Read from s3.
   *
   * @param {Key} key
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   */
  get (key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    callback = once(callback)
    super.get(key,callback);     
  }

  /**
   * Check for the existence of the given key in the cache if the current instance is handling blocks,
   * otherwise it checks s3.
   *
   * @param {Key} key
   * @param {function(Error, bool)} callback
   * @returns {void}
   */
  has (key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    callback = once(callback);
    try {
      if (_.includes(this.path ,'/blocks') && this.cacheReady) {
        (this.indexCache.get(key.toString())) ? callback(undefined, true) : callback(undefined, false);
      } else {
        super.has(key,callback);
      }     
    } catch (error) {
      callback(error, false);
    }
  }

  /**
   * Delete the record under the given key in the cache and s3.
   *
   * @param {Key} key
   * @param {function(Error)} callback
   * @returns {void}
   */
  delete (key /* : Key */, callback /* : Callback<void> */) /* : void */ {
    callback = once(callback);
    super.delete(key,callback);
    this.indexCache.del(key);  
  }

  /**
   * Create a new batch object.
   *
   * @returns {Batch}
   */
  batch () /* : Batch<Buffer> */ {
    return super.batch();
  }

  /**
   * Recursively fetches all keys from s3
   * @param {Object} params
   * @param {Array<Key>} keys
   * @param {function} callback
   * @returns {void}
   */
  _listKeys (params /* : { Prefix: string, StartAfter: ?string } */, keys /* : Array<Key> */, callback /* : Callback<void> */) {
    callback = once(callback)
    super._listKeys(params, keys, callback)
  }

  _listInitialKeys (params /* : { Prefix: string, StartAfter: ?string } */, keys /* : Array<Key> */, callback /* : Callback<void> */) {
    if (typeof callback === 'undefined') {
      callback = keys
      keys = []
    }
    callback = once(callback)
    this.opts.s3.listObjectsV2(params, (err, data) => {
      if (err) {
        return callback(new Error(err.code))
      }
      data.Contents.forEach((d) => {
        // Remove the path from the key
        keys.push(d.Key.slice(params.Prefix.length));
      });
      // If we didnt get all records, recursively query
      if (data.isTruncated) {
        // If NextMarker is absent, use the key from the last result
        params.StartAfter = data.Contents[data.Contents.length - 1].Key
        // recursively fetch keys
        return this._listKeys(params, keys, callback)
      }
      callback(err, keys)
    })
  }

  /**
   * Returns an iterator for fetching objects from s3 by their key
   * @param {Array<Key>} keys
   * @param {Boolean} keysOnly Whether or not only keys should be returned
   * @returns {Iterator}
   */
  _getS3Iterator (keys /* : Array<Key> */, keysOnly /* : boolean */) {
   super._getS3Iterator(keys,keysOnly)
  }

  /**
   * Query the store.
   *
   * @param {Object} q
   * @returns {PullStream}
   */
  query (q /* : Query<Buffer> */) /* : QueryResult<Buffer> */ {
    return super.query(q);
  }

  /**
   * This will check the s3 bucket to ensure access and existence
   *
   * @param {function(Error)} callback
   * @returns {void}
   */
  open (callback /* : Callback<void> */) /* : void */ {
    callback = once(callback)
    super.open(callback)
  }
  /**
   * Close the store.
   *
   * @param {function(Error)} callback
   * @returns {void}
   */
  close (callback /* : (err: ?Error) => void */) /* : void */ {
    callback = once(callback)
    super.close(callback);
  }
}

module.exports = cachedS3Datastore

