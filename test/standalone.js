// execute this script with a redis container running to test the health check
// starting and stopping redis with this script running is a good test

const redis = require('../')

const rclient = redis.createClient({})

setInterval(() => {
  rclient.healthCheck(err => {
    if (err) {
      console.error('HEALTH CHECK FAILED', err)
    } else {
      console.log('HEALTH CHECK OK')
    }
  })
}, 1000)
