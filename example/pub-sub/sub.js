let ZWorker = require("./index");

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
  console.log(e);
});


let consumer = zWorker.consumer("sample", { prefetch: 1 });

consumer.sub("message", (msgs) => {
  console.log("Message from queue")
  msgs.forEach(msg => {
    console.log(msg)
    msg.ack();
  });
});
