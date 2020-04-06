_ = require("underscore")
os = require('os')
crypto = require('crypto')

{
	RedisHealthCheckTimedOut,
	RedisHealthCheckWriteError,
	RedisHealthCheckVerifyError,
} = require('./Errors')

# generate unique values for health check
HOST = os.hostname()
PID = process.pid
RND = crypto.randomBytes(4).toString('hex')
COUNT = 0

module.exports = RedisSharelatex =
	createClient: (opts = {port: 6379, host: "localhost"})->
		if !opts.retry_max_delay?
			opts.retry_max_delay = 5000 # ms

		if opts.endpoints?
			standardOpts = _.clone(opts)
			delete standardOpts.endpoints
			delete standardOpts.masterName
			client = require("redis-sentinel").createClient opts.endpoints, opts.masterName, standardOpts
			client.healthCheck = RedisSharelatex.singleInstanceHealthCheckBuilder(client)
		else if opts.cluster?
			Redis = require("ioredis")
			standardOpts = _.clone(opts)
			delete standardOpts.cluster
			delete standardOpts.key_schema
			client = new Redis.Cluster(opts.cluster, standardOpts)
			client.healthCheck = RedisSharelatex.clusterHealthCheckBuilder(client)
			RedisSharelatex._monkeyPatchIoredisExec(client)
		else
			standardOpts = _.clone(opts)
			ioredis = require("ioredis")
			client = new ioredis(standardOpts)
			RedisSharelatex._monkeyPatchIoredisExec(client)
			client.healthCheck = RedisSharelatex.singleInstanceHealthCheckBuilder(client)

		return client
	
	HEARTBEAT_TIMEOUT: 2000
	singleInstanceHealthCheckBuilder: (client) ->
		healthCheck = (callback) ->
			RedisSharelatex._checkClient(client, callback)
		return healthCheck
	
	clusterHealthCheckBuilder: (client) ->
		return RedisSharelatex.singleInstanceHealthCheckBuilder(client)
	
	_checkClient: (client, callback) ->
		callback = _.once(callback)
		# check the redis connection by storing and retrieving a unique key/value pair
		uniqueToken = "host=#{HOST}:pid=#{PID}:random=#{RND}:time=#{Date.now()}:count=#{COUNT++}"

		# o-error context
		context = {
			uniqueToken,
			clusterStartupNodes: client.startupNodes,
			clientOptions: client.options,
			stage: 'add context for a timeout'
		}

		timer = setTimeout () ->
			callback(new RedisHealthCheckTimedOut({
				message: 'timeout', info: context
			}))
		, RedisSharelatex.HEARTBEAT_TIMEOUT

		healthCheckKey = "_redis-wrapper:healthCheckKey:{#{uniqueToken}}"
		healthCheckValue = "_redis-wrapper:healthCheckValue:{#{uniqueToken}}"
		# set the unique key/value pair
		context.stage = 'write'
		multi = client.multi()
		multi.set healthCheckKey, healthCheckValue, "EX", 60
		multi.exec (err, reply) ->
			if err?
				clearTimeout timer
				return callback(new RedisHealthCheckWriteError({
					message: 'write multi errored', info: context
				}).withCause(err))
			if not reply or reply[0] is not 'OK'
				clearTimeout timer
				context.reply = reply
				return callback(new RedisHealthCheckWriteError({
					message: 'write failed', info: context
				}))

			# check that we can retrieve the unique key/value pair
			context.stage = 'verify'
			multi = client.multi()
			multi.get healthCheckKey
			multi.del healthCheckKey
			multi.exec (err, reply) ->
				clearTimeout timer
				if err?
					return callback(new RedisHealthCheckVerifyError({
						message: 'get/del multi errored', info: context
					}).withCause(err))
				if not reply or reply[0] isnt healthCheckValue or reply[1] isnt 1
					context.reply = reply
					return callback(new RedisHealthCheckVerifyError({
						message: 'read/delete failed', info: context
					}))
				return callback()

	_monkeyPatchIoredisExec: (client) ->
		_multi = client.multi
		client.multi = (args...) ->
			multi = _multi.call(client, args...)
			_exec = multi.exec
			multi.exec = (callback = () ->) ->
				_exec.call multi, (error, result) ->
					# ioredis exec returns an results like:
					# [ [null, 42], [null, "foo"] ]
					# where the first entries in each 2-tuple are
					# presumably errors for each individual command,
					# and the second entry is the result. We need to transform
					# this into the same result as the old redis driver:
					# [ 42, "foo" ]
					filtered_result = []
					for entry in result or []
						if entry[0]?
							return callback(entry[0])
						else
							filtered_result.push entry[1]
					callback error, filtered_result
			return multi
	
		
	

