
# zWorker

async Job processor / pub-sub using pgsql as datastore


## Installation

Install my-project with npm

```bash
  npm install zworker
```
    
## Usage/Examples
### pub-sub

#### publisher.js
```javascript
// publisher.js

let ZWorker = require("zworker");

let zWorker = new ZWorker(
  {
    DB_NAME: "db-name",
    DB_HOST: "db-host",
    DB_USER: "db-user",
    DB_PASSWORD: "db-password",
    DB_PORT: 5432,
  },
  true
);

zWorker.on("error", (e) => {
  console.log(e.toString());
});

zWorker.on("connect", (e) => {
  console.log('[PUB] zworker client connected!')

  zWorker.pub({
    name: "sample",                     // name of queue
    message: "Hello world !",           // any string message
  });

});
```


#### subscriber.js
```javascript
// subscriber.js

let ZWorker = require("zworker");

let zWorker = new ZWorker(
  {
    DB_NAME: "db-name",
    DB_HOST: "db-host",
    DB_USER: "db-user",
    DB_PASSWORD: "db-password",
    DB_PORT: 5432,
  },
  true
);

zWorker.on("error", (e) => {
  console.log(e.toSring());
});

zWorker.on("connect", (e) => {
   console.log('[SUB] zworker client connected!')
});


let consumer = zWorker.consumer("sample", { prefetch: 1 });

consumer.sub("message", (msgs) => {
  console.log("[QUEUE] New Message :>")

  msgs.forEach(msg => {
    console.log(msg)
    msg.ack(); // msg needs to be explicitly acked once processing is done;
  });

});

```
## Authors

- [@sjljrvis](https://github.com/sjljrvis)

