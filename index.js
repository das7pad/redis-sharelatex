const crypto = require('crypto')
const os = require('os')

const IORedis = require('ioredis')
const lodashOnce = require('lodash.once')

const {
  RedisHealthCheckTimedOut,
  RedisHealthCheckWriteError,
  RedisHealthCheckVerifyError
} = require('./Errors')

const HEARTBEAT_TIMEOUT = 2000

// generate unique values for health check
const HOST = os.hostname()
const PID = process.pid
const RND = crypto.randomBytes(4).toString('hex')
let COUNT = 0

function healthCheckBuilder(client) {
  return callback => healthCheck(client, callback)
}

function healthCheck(client, callback) {
  callback = lodashOnce(callback)

  // check the redis connection by storing and retrieving a unique key/value pair
  const uniqueToken = `host=${HOST}:pid=${PID}:random=${RND}:time=${Date.now()}:count=${COUNT++}`

  // o-error context
  const context = {
    timeout: HEARTBEAT_TIMEOUT,
    uniqueToken,
    clusterStartupNodes: client.startupNodes,
    clientOptions: client.options,
    stage: 'add context for a timeout'
  }

  const timer = setTimeout(() => {
    callback(
      new RedisHealthCheckTimedOut({
        message: 'timeout',
        info: context
      })
    )
  }, HEARTBEAT_TIMEOUT)

  const healthCheckKey = `_redis-wrapper:healthCheckKey:{${uniqueToken}}`
  const healthCheckValue = `_redis-wrapper:healthCheckValue:{${uniqueToken}}`
  // set the unique key/value pair
  context.stage = 'write'
  let multi = client.multi()
  multi.set(healthCheckKey, healthCheckValue, 'EX', 60)
  multi.exec((err, reply) => {
    if (err) {
      clearTimeout(timer)
      return callback(
        new RedisHealthCheckWriteError({
          message: 'write multi errored',
          info: context
        }).withCause(err)
      )
    }
    if (!reply || reply[0] !== 'OK') {
      clearTimeout(timer)
      context.reply = reply
      return callback(
        new RedisHealthCheckWriteError({
          message: 'write failed',
          info: context
        })
      )
    }

    // check that we can retrieve the unique key/value pair
    context.stage = 'verify'
    multi = client.multi()
    multi.get(healthCheckKey)
    multi.del(healthCheckKey)
    multi.exec((err, reply) => {
      clearTimeout(timer)
      if (err) {
        return callback(
          new RedisHealthCheckVerifyError({
            message: 'get/del multi errored',
            info: context
          }).withCause(err)
        )
      }
      if (!reply || reply[0] !== healthCheckValue || reply[1] !== 1) {
        context.reply = reply
        return callback(
          new RedisHealthCheckVerifyError({
            message: 'read/delete failed',
            info: context
          })
        )
      }
      callback()
    })
  })
}

function monkeyPatchIoRedisExec(client) {
  const _multi = client.multi
  client.multi = () => {
    const multi = _multi.apply(client, arguments)
    const _exec = multi.exec
    multi.exec = (callback = () => {}) =>
      _exec.call(multi, (error, result) => {
        // ioredis exec returns an results like:
        // [ [null, 42], [null, "foo"] ]
        // where the first entries in each 2-tuple are
        // presumably errors for each individual command,
        // and the second entry is the result. We need to transform
        // this into the same result as the old redis driver:
        // [ 42, "foo" ]
        const filtered_result = []
        for (const entry of result || []) {
          if (entry[0]) {
            return callback(entry[0])
          } else {
            filtered_result.push(entry[1])
          }
        }
        callback(error, filtered_result)
      })
    return multi
  }
}

module.exports = {
  createClient: (opts = {}) => {
    const standardOpts = require('lodash.clone')(opts)
    delete standardOpts.key_schema

    let client
    if (opts.cluster) {
      delete standardOpts.cluster
      client = new IORedis.Cluster(opts.cluster, standardOpts)
    } else {
      client = new IORedis(standardOpts)
    }
    monkeyPatchIoRedisExec(client)
    client.healthCheck = healthCheckBuilder(client)
    return client
  }
}
