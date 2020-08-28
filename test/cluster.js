/*
  execute this script with a redis cluster running to test the health check.
  starting and stopping shards with this script running is a good test.

  to create a new cluster, use $ ./create-redis-cluster.sh

  one chaos monkey stage could be deleting all keys:
$ while true;do seq 0 8 | xargs -I% redis-cli -p 700% FLUSHALL > /dev/null;done
*/

const redis = require('../')

const rclient = redis.createClient({
  cluster: Array.from({ length: 9 }).map((value, index) => {
    return { host: '127.0.0.1', port: 7000 + index }
  })
})

setInterval(() => {
  rclient.healthCheck(err => {
    if (err) {
      console.error('HEALTH CHECK FAILED', JSON.stringify(err, null, 2))
    } else {
      console.log('HEALTH CHECK OK')
    }
  })
}, 1000)
