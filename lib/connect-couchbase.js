/*!
 * Connect - Couchbase
 * Copyright(c) 2014 Christopher Mina <christopher.mina@gmail.com>
 *
 * MIT Licensed
 *
 * This is an adaption from connect-redis, see:
 * https://github.com/visionmedia/connect-redis
 */

'use strict'

/**
 * Module dependencies.
 */

var debug = require('debug')('connect:couchbase');

/**
 * One day in seconds.
 */

var oneDay = 86400;

/**
 * No op
 */
var noop = function () {};

/**
 * Return the `CouchbaseStore` extending `express`'s session Store.
 *
 * @param {object} express session
 * @return {Function}
 * @api public
 */

module.exports = function(session){

    /**
     * Express's session Store.
     */

    var Store = session.Store;

    /**
     * Initialize CouchbaseStore with the given `options`.
     *
     * @param {Object} options
     *      {
     *          host: 127.0.0.1:8091 (default) -- Can be one or more address:ports, separated by semi-colon, or an array
     *          username: '',   -- Should be same as bucket name, if provided
     *          password: '',
     *          bucket: 'default' (default)
     *          cachefile: ''
     *          ttl: 86400,
     *          prefix: 'sess',
     *          operationTimeout:2000,
                connectionTimeout:2000,
     *      }
     * @api public
     */

    function CouchbaseStore(options) {
        var self = this;

        options = options || {};
        Store.call(this, options);
        this.prefix = null == options.prefix
            ? 'sess:'
            : options.prefix;

        var connectOptions = {};
        if (options.hasOwnProperty("host")) {
            connectOptions.host = options.host;
        } else if (options.hasOwnProperty("hosts")) {
            connectOptions.host = options.hosts;
        }

        if (options.hasOwnProperty("username")) {
            connectOptions.username = options.username;
        }

        if (options.hasOwnProperty("password")) {
            connectOptions.password = options.password;
        }

        if (options.hasOwnProperty("bucket")) {
            connectOptions.bucket = options.bucket;
        }

        if (options.hasOwnProperty("cachefile")) {
            connectOptions.cachefile = options.cachefile;
        }

        if (options.hasOwnProperty("connectionTimeout")) {
            connectOptions.connectionTimeout = options.connectionTimeout;
        }

        if (options.hasOwnProperty("operationTimeout")) {
            connectOptions.operationTimeout = options.operationTimeout;
        }

        if (options.hasOwnProperty("collectionName")) {
            connectOptions.collectionName = options.collectionName;
        }

        if (options.hasOwnProperty("db")) {
            connectOptions.db = options.db; // DB Instance
        }

        if ( typeof(connectOptions.db) != 'undefined' ) {
            this.client = connectOptions.db;
        } else {
            var Couchbase = require('couchbase');
            Couchbase.Cluster.connect(
              connectOptions.host,
              {
                username: connectOptions.username,
                password: connectOptions.password
              }
            ).then((cluster) => {
                var bucket = cluster.bucket(connectOptions.bucket);
                var prefix = this.prefix;

                this.client = connectOptions.connectionName
                    ? bucket.defaultCollection()
                    : bucket.collection(connectOptions.collectionName);

                this.queryAll = 'SELECT `'
                    + connectOptions.bucket + '`.* FROM `'
                    + connectOptions.bucket + '` WHERE SUBSTR(META(`'
                    + connectOptions.bucket + '`).id, 0, '
                    + prefix.length + ') = "'
                    + prefix + '"';

                // disconnect
                this.client.disconnect = (err) => {
                    cluster.close();
                    self.emit('disconnect', err);
                }
                self.emit('connect');
            }).catch((err) => {
                console.error("Could not connect to couchbase with bucket: " + connectOptions.bucket);
                self.emit('disconnect', err);
            });
        }

        this.operationTimeout = connectOptions.operationTimeout || 10000;
        this.ttl = options.ttl || null;
    }

    /**
     * Inherit from `Store`.
     */

    CouchbaseStore.prototype.__proto__ = Store.prototype;

    /**
     * Attempt to fetch session by the given `sid`.
     *
     * @param {String} sid
     * @param {Function} fn
     * @api public
     */

    CouchbaseStore.prototype.get = function(sid, fn){
        if ('function' !== typeof fn) { fn = noop; }
        sid = this.prefix + sid;
        debug('GET "%s"', sid);
        this.client.get(sid, { timeout: this.operationTimeout })
            .then(data => {
                if (!data || !data.value) return fn();
                var result;
                data = data.value.toString();
                debug('GOT %s', data);
                try {
                    result = JSON.parse(data);
                } catch (err) {
                    return fn(err);
                }
                return fn(null, result);
            })
            .catch(err => {
                //Handle Key Not Found error
                if (err && err.code == 13) {
                    return fn(null);
                }
                if (err) return fn(err);
            });
    };

    /**
     * Commit the given `sess` object associated with the given `sid`.
     *
     * @param {String} sid
     * @param {Session} sess
     * @param {Function} fn
     * @api public
     */

    CouchbaseStore.prototype.set = function(sid, sess, fn){
        if ('function' !== typeof fn) { fn = noop; }
        sid = this.prefix + sid;
        try {
            var maxAge = sess.cookie.maxAge
                , ttl = this.ttl
                , sess = JSON.stringify(sess);

            ttl = ttl || ('number' == typeof maxAge
                ? maxAge / 1000 | 0
                : oneDay);

            debug('SETEX "%s" ttl:%s %s', sid, ttl, sess);
            this.client.upsert(sid, sess, { expiry: ttl, timeout: this.operationTimeout })
                .then(result => {
                    debug("Session Set %s complete", sid);
                    fn(null, sess);
                })
                .catch(fn);
        } catch (err) {
            fn && fn(err);
        }
    };

    /**
     * Returns all active session
     *
     * @param {Function} fn
     * @api public
     */

    CouchbaseStore.prototype.all = function(fn){
        this.client.query(this.queryAll, [], { readonly: true, timeout: this.operationTimeout })
            .then(result => {
                fn(null, result.rows);
            })
            .catch(fn);
    };

    /**
     * Destroy the session associated with the given `sid`.
     *
     * @param {String} sid
     * @api public
     */

    CouchbaseStore.prototype.destroy = function(sid, fn){
        if ('function' !== typeof fn) { fn = noop; }
        sid = this.prefix + sid;
        this.client.remove(sid)
            .then((result) => {
                debug("Session Destroy %s complete", sid);
                fn(null, result);
            })
            .catch(fn);
    };


    /**
     * Refresh the time-to-live for the session with the given `sid`.
     *
     * @param {String} sid
     * @param {Session} sess
     * @param {Function} fn
     * @api public
     */

    CouchbaseStore.prototype.touch = function (sid, sess, fn) {
        if ('function' !== typeof fn) { fn = noop; }

        var maxAge = sess.cookie.maxAge
            , ttl = this.ttl
            , sess = JSON.stringify(sess);

        ttl = ttl || ('number' == typeof maxAge
                ? maxAge / 1000 | 0
                : oneDay);

        debug('EXPIRE "%s" ttl:%s', sid, ttl);
        this.client.touch(this.prefix + sid, ttl, { timeout: this.operationTimeout })
            .then(result => {
                debug("Session Touch %s complete", sid);
                fn(null, result);
            })
            .catch(fn);
    };

    return CouchbaseStore;
};
