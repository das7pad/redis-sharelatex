const crypto = require('crypto')
const os = require('os')
const { promisify } = require('util')

const IORedis = require('ioredis')

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

function healthCheck(client, callback) {
  // check the redis connection by storing and retrieving a unique key/value pair
  const uniqueToken = `host=${HOST}:pid=${PID}:random=${RND}:time=${Date.now()}:count=${COUNT++}`

  // o-error context
  const context = {
    uniqueToken,
    stage: 'add context for a timeout'
  }

  let healthCheckDeadline
  Promise.race([
    new Promise((resolve, reject) => {
      healthCheckDeadline = setTimeout(() => {
        context.timeout = HEARTBEAT_TIMEOUT
        reject(new RedisHealthCheckTimedOut('timeout'))
      }, HEARTBEAT_TIMEOUT)
    }),
    runCheck(client, uniqueToken, context).finally(() =>
      clearTimeout(healthCheckDeadline)
    )
  ])
    .then(() => {
      callback()
    })
    .catch((err) => {
      // attach the o-error context
      err.info = context
      callback(err)
    })
}

async function runCheck(client, uniqueToken, context) {
  let reply, multi
  const healthCheckKey = `_redis-wrapper:healthCheckKey:{${uniqueToken}}`
  const healthCheckValue = `_redis-wrapper:healthCheckValue:{${uniqueToken}}`

  // set the unique key/value pair
  context.stage = 'write'
  multi = client.multi()
  multi.set(healthCheckKey, healthCheckValue, 'EX', 60)
  reply = await multi.exec().catch((err) => {
    throw new RedisHealthCheckWriteError('multi errored').withCause(err)
  })
  if (reply[0] !== 'OK') {
    context.reply = reply
    throw new RedisHealthCheckWriteError('write failed')
  }

  // check that we can retrieve the unique key/value pair
  context.stage = 'verify'
  multi = client.multi()
  multi.get(healthCheckKey)
  multi.del(healthCheckKey)
  reply = await multi.exec().catch((err) => {
    throw new RedisHealthCheckVerifyError('multi errored').withCause(err)
  })
  if (reply[0] !== healthCheckValue || reply[1] !== 1) {
    context.reply = reply
    throw new RedisHealthCheckVerifyError('read/delete failed')
  }
}

function unwrapMultiResult(result, callback) {
  // ioredis exec returns a results like:
  // [ [null, 42], [null, "foo"] ]
  // where the first entries in each 2-tuple are
  // presumably errors for each individual command,
  // and the second entry is the result. We need to transform
  // this into the same result as the old redis driver:
  // [ 42, "foo" ]
  //
  // Basically reverse:
  // https://github.com/luin/ioredis/blob/v4.17.3/lib/utils/index.ts#L75-L92
  const filteredResult = []
  for (const [err, value] of result || []) {
    if (err) {
      return callback(err)
    } else {
      filteredResult.push(value)
    }
  }
  callback(null, filteredResult)
}
const unwrapMultiResultPromisified = promisify(unwrapMultiResult)

function monkeyPatchIoRedisExec(client) {
  const _multi = client.multi
  client.multi = () => {
    const multi = _multi.apply(client, arguments)
    const _exec = multi.exec
    multi.exec = (callback) => {
      if (callback) {
        _exec.call(multi, (error, result) => {
          // The command can fail all-together due to syntax errors
          if (error) return callback(error)
          unwrapMultiResult(result, callback)
        })
      } else {
        return _exec.call(multi).then(unwrapMultiResultPromisified)
      }
    }
    return multi
  }
}

module.exports = {
  createClient: (opts = {}) => {
    const standardOpts = Object.assign({}, opts)
    delete standardOpts.key_schema

    let client
    if (opts.cluster) {
      delete standardOpts.cluster
      client = new IORedis.Cluster(opts.cluster, standardOpts)
    } else {
      client = new IORedis(standardOpts)
    }
    monkeyPatchIoRedisExec(client)
    client.healthCheck = (callback) => healthCheck(client, callback)
    return client
  }
}
