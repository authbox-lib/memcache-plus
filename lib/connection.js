'use strict';
/**
 * @file Main file for a Memcache Connection
 */

var debug = require('debug')('memcache-plus:connection');

var _ = require('lodash'),
    assert = require('chai').assert,
    misc = require('./misc'),
    net = require('net'),
    Promise = require('bluebird'),
    Queue = require('collections/deque'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter;

const MAGIC_REQUEST = 0x80;
const MAGIC_RESPONSE = 0x81;

const HEADER_SIZE = 24;

const CMD_GET = 0;
const CMD_SET = 1;
const CMD_ADD = 2;
const CMD_REPLACE = 3;
const CMD_DELETE = 4;
const CMD_INCREMENT = 5;
const CMD_DECREMENT = 6;
const CMD_QUIT = 7;
const CMD_FLUSH = 8;
const CMD_GETQ = 9;
const CMD_NOOP = 10;
const CMD_VERSION = 11;
const CMD_GETK = 12;
const CMD_GETKQ = 13;
const CMD_APPEND = 14;
const CMD_PREPEND = 15;
const CMD_STAT = 16;
const CMD_SETQ = 17;
const CMD_ADDQ = 18;
const CMD_REPLACEQ = 19;
const CMD_DELETEQ = 20;
const CMD_INCREMENTQ = 21;
const CMD_DECREMENTQ = 22;
const CMD_QUITQ = 23;
const CMD_FLUSHQ = 24;
const CMD_APPENDQ = 25;
const CMD_PREPENDQ = 26;

const RESP_NO_ERROR = 0x00;
const RESP_KEY_NOT_FOUND = 0x01;
const RESP_KEY_EXISTS  = 0x02;
const RESP_VALUE_TOO_LARGE = 0x03;
const RESP_INVALID_ARGUMENTS = 0x04;
const RESP_ITEM_NOT_STORED = 0x05;
const RESP_INCR_DECR_ON_NON_NUMERIC = 0x06;

const RESP_UNKNOWN_COMMAND = 0x81;
const RESP_OUT_OF_MEMORY = 0x82;

const emptyBuffer = new Buffer(0);

/**
 * Connection constructor
 *
 * With the supplied options, connect.
 *
 * @param {object} opts - The options for this Connection instance
 */
function Connection(opts) {


    EventEmitter.call(this);

    opts = opts || {};

    _.defaults(opts, {
        host: 'localhost',
        port: '11211',
        reconnect: true
    });

    this.host = opts.host;
    this.port = opts.port;

    this.queue = new Queue();

    if (opts.onConnect) {
        this.onConnect = opts.onConnect;
    }

    if (opts.onError) {
        this.onError = opts.onError;
    } else {
        this.onError = function onError(err) { this.emit('error'); console.error(err); };
    }
    this.netTimeout = opts.netTimeout || 500;
    this.backoffLimit = opts.backoffLimit || 10000;
    this.reconnect = opts.reconnect;
    this.disconnecting = false;
    this.ready = false;
    this.backoff = opts.backoff || 10;

    this.connect();
}

util.inherits(Connection, EventEmitter);

/**
 * Disconnect connection
 */
Connection.prototype.disconnect = function() {
    this.ready = false;
    this.disconnecting = true;
    if (this.client) {
      this.client.end();
    }
};

/**
 * Destroy connection immediately
 */
Connection.prototype.destroy = function(message) {
    var self = this;

    debug('destroying connection: %s', message);

    this.client.destroy();
    this.client = null;

    this.queue.forEach(function(deferred) {
        deferred.reject(new Error(message || 'Memcache connection lost'));
    });
    this.queue.clear();
};

/**
 * Handle protocol errors
 */
Connection.prototype.protocolError = function(err) {
  this.destroy(`Memcache protocol error: ${err.toString()}`);
  this.onError(err);
}

/**
 * Initialize connection
 *
 * @api private
 */
Connection.prototype.connect = function() {
    var params = {
        port: this.port
    };

    if (this.host) {
        params.host = this.host;
    }

    debug('connecting to host %s:%s', params.host, params.port);

    // If a client already exists, we just want to reconnect
    if (this.client) {
        this.client.connect(params);

    } else {
        // Initialize a new client, connect
        this.client = net.connect(params);
        this.client.setTimeout(this.netTimeout);
        this.client.on('error', this.onError.bind(this));
        this.client.setNoDelay(true);

        // If reconnect is enabled, we want to re-initiate connection if it is ended
        if (this.reconnect) {
            this.client.on('close', () => {
                this.emit('close');
                this.ready = false;
                // Wait before retrying and double each time. Backoff starts at 10ms and will
                // plateau at 1 minute.
                if (this.backoff < this.backoffLimit) {
                    this.backoff *= 2;
                }
                debug('connection to memcache lost, reconnecting in %sms...', this.backoff);
                setTimeout(() => {
                    // Only want to do this if a disconnect was not triggered intentionally
                    if (!this.disconnecting) {
                        debug('attempting to reconnect to memcache now.', this.backoff);
                        this.destroy();
                        this.connect();
                    }
                }, this.backoff);
            });
        }
    }

    this.client.on('connect', () => {
        this.emit('connect');
        debug('successfully (re)connected!');
        this.ready = true;
        // Reset backoff if we connect successfully
        this.backoff = 10;

        // If an onConnect handler was specified, execute it
        if (this.onConnect) {
            this.onConnect();
        }
        this.flushQueue();
    });

    let unprocessed = null;
    this.client.on('data', (buffer) => {
      try {
        // memcache should manange get entire requests in a single packet; this should be rare.
        if (unprocessed) {
          buffer = Buffer.concat([unprocessed, buffer]);
          unprocessed = null;
        }

        let offset = 0;
        while (buffer.length - offset >= HEADER_SIZE) {
          assert(buffer[offset] == MAGIC_RESPONSE, 'Memcache error: did not see response magic');

          // Magic        (0)
          // Opcode       (1)
          // Key length   (2,3)
          // Extra length (4)
          // Data type    (5)
          // Status       (6,7)
          // Total body   (8-11)
          // Opaque       (12-15)
          // CAS          (16-23)
          const bodyLength = buffer.readUInt32BE(offset + 8, true);
          const keyLength = buffer.readUInt16BE(offset + 2, true);
          const extraLength = buffer.readUInt8(offset + 4, true);
          const responseStatus = buffer.readUInt16BE(offset + 6, true);
          const valueLength = bodyLength - (extraLength + keyLength);
          if (buffer.length - offset < HEADER_SIZE + bodyLength) {
            break;
          }

          assert(valueLength >= 0, 'Memcache protocol error: invalid value length');
          offset += HEADER_SIZE + extraLength;
          const key = buffer.slice(offset, offset + keyLength);
          offset += keyLength;
          const value = buffer.slice(offset, offset + valueLength);
          offset += valueLength;

          this.process(responseStatus, key, value);
        }

        // Save the rest of the data if there is any
        if (offset < buffer.length) {
          unprocessed = buffer.slice(offset);
        }
      } catch (err) {
        this.protocolError(err);
      }
    });
};

Connection.prototype.process = function(status, key, value) {
    var deferred = this.queue.shift();
    if (deferred === undefined) {
        this.protocolError(new Error('Received unexpected data'));
        return;
    }

    deferred.resolve([status, key, value]);
};

Connection.prototype.flushQueue = function() {
    if (this.writeBuffer && this.writeBuffer.length > 0) {
        debug('flushing connection write buffer');
        // @todo Watch out for and handle how this behaves with a very long buffer
        while(this.writeBuffer.length > 0) {
            this.client.write(this.writeBuffer.shift());
        }
    }
};

Connection.prototype.write = function(buffer) {
    debug('sending data: %s', buffer.toString('hex'));
    this.writeBuffer = this.writeBuffer || new Queue();
    // If for some reason this connection is not yet ready and a request is tried,
    // we don't want to fire it off so we write it to a buffer and then will fire
    // them off when we finally do connect. And even if we are connected we don't
    // want to fire off requests unless the write buffer is emptied. So if say we
    // buffer 100 requests, then connect and chug through 10, there are 90 left to
    // be flushed before we send it new requests so we'll just keep pushing on the
    // end until it's flushed
    if (this.ready && this.writeBuffer.length < 1) {
        this.client.write(buffer);
    } else if (this.writeBuffer.length < 1000) {
        this.writeBuffer.push(buffer);
        // Check if we should flush this queue. Useful in case it gets stuck for
        // some reason
        if (this.ready) {
            this.flushQueue();
        }
    } else {
        this.queue.shift().reject('Error, Connection to memcache lost and buffer over 1000 items');
    }
};

Connection.prototype.raw = function(opcode, extra, key, val) {
  // Automatically convert key/value to buffers
  if (typeof key === 'string') {
    key = new Buffer(key, 'utf8');
  }
  if (typeof val === 'string') {
    val = new Buffer(val, 'utf8');
  }

  assert(extra instanceof Buffer, 'Memcache extra data must be a buffer');
  assert(extra.length < 256, 'Memcache extra length limited at 255 bytes');
  assert(key instanceof Buffer, 'Memcache keys must be a string/buffer key');
  assert(val instanceof Buffer, 'Memcache values must be a string/buffer value');
  assert(key.length < Math.pow(2, 16), 'Memcache key must be less than 65536 characters long');
  assert(key.length + val.length < Math.pow(2, 32), 'Memcache data size too large');

  const pending = Promise.pending();
  this.queue.push(pending);

  // @TODO: allocUnsafe
  const header = new Buffer(24);

  // Magic        (0)
  header.writeUInt8(MAGIC_REQUEST, 0, true);
  // Opcode       (1)
  header.writeUInt8(opcode, 1, true);
  // Key length   (2,3)
  header.writeUInt16BE(key.length, 2, true);
  // Extra length (4)
  header.writeUInt8(extra.length, 4, true);
  // Data type    (5)
  header.writeUInt8(0, 5, true);
  // Reserved     (6,7)
  header.writeUInt16BE(0, 6, true);
  // Total body   (8-11)
  header.writeUInt32BE(extra.length + key.length + val.length, 8, true);
  // Opaque       (12-15)
  header.writeUInt32BE(0, 12, true);
  // CAS          (16-23)
  header.writeUInt32BE(0, 16, true);
  header.writeUInt32BE(0, 20, true);

  this.write(header);
  this.write(extra);
  this.write(key);
  this.write(val);

  return pending.promise;
}

Connection.prototype.autodiscovery = function() {
    debug('starting autodiscovery');
    var deferred = misc.defer('autodiscovery');
    this.queue.push(deferred);

    throw new Error('@TODO: autodiscovery disabled.');
    // this.write('config get cluster');

    return deferred.promise
        .then(function(data) {
            debug('got autodiscovery response from elasticache');
            // Elasticache returns hosts as a string like the following:
            // victor.di6cba.0001.use1.cache.amazonaws.com|10.10.8.18|11211 victor.di6cba.0002.use1.cache.amazonaws.com|10.10.8.133|11211
            // We want to break it into the correct pieces
            var hosts = data.toString().split(' ');
            return hosts.map(function(host) {
                host = host.split('|');
                return util.format('%s:%s', host[0], host[2]);
            });
        });
};

/**
 * set() - Set a value on this connection
 */
Connection.prototype.set = function(key, val, ttl) {
  debug('set %s:%s', key, val);

  ttl = ttl || 0;
  var opts = {};

  if (_.isObject(ttl)) {
      opts = ttl;
      ttl = opts.ttl || 0;
  }

  // flags(4), expiry(4)
  const extra = new Buffer(8);
  extra.writeUInt32BE(0, 0, true);
  extra.writeUInt32BE(ttl, 4, true);

  return this.raw(CMD_SET, extra, key, val).spread((status, resultKey, value) => {
    if (status === RESP_NO_ERROR) {
      return;
    } else {
      throw new Error(`Unexpected memcache set response: ${status}`);
    }
  });
};

/**
 * get() - Get a value on this connection
 *
 * @param {String} key - The key for the value to retrieve
 * @param {Object} [opts] - Any additional options for this get
 * @returns {Promise}
 */
Connection.prototype.get = function(key, opts) {
  debug('get %s', key);
  opts = opts || {};

  return this.raw(CMD_GET, emptyBuffer, key, emptyBuffer).spread((status, resultKey, value) => {
    debug('get %s: status %d', key, status);

    if (status === RESP_KEY_NOT_FOUND) {
      return null;
    } else if (status === RESP_NO_ERROR) {
      return value;
    } else {
      throw new Error(`Unexpected memcache get response: ${status}`);
    }
  });
};

/**
 * delete() - Delete value for this key on this connection
 *
 * @param {String} key - The key to delete
 * @returns {Promise}
 */
Connection.prototype.delete = function(key) {
  debug('delete %s', key);

  return this.raw(CMD_DELETE, emptyBuffer, key, emptyBuffer).spread((status, resultKey, value) => {
    if (status === RESP_NO_ERROR) {
      return true;
    } else if (status === RESP_KEY_NOT_FOUND) {
      return false;
    } else {
      throw new Error(`Unexpected memcache delete response: ${status}`);
    }
  });
};

module.exports = Connection;
