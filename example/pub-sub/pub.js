let ZWorker = require("../../index");

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
  console.log('Publisher working')
  zWorker.pub({
    name: "sample",
    message: "Hello world !",
  });
});

