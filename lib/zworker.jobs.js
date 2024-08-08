const { Client: PgClient } = require("pg");
const debug = require("debug")("zworker:jobs");
const EventEmitter = require("events");
const { dbOptionsSample } = require("./validation");
const Message = require("./message");
const {
  createTables,
  insertTask,
  fetchAndRun,
  publish,
  subscribe,
} = require("./database");

class ZWorker extends EventEmitter {

  #dbOptions;
  #dbClient;
  #executorRunning;
  #consumerRunning;
  #consumers;
  #queueInterval;

  constructor(dbOptions, executorEnabled = true) {
    super();
    let isValid = this.#validate(arguments);
    if (isValid) {
      this.#consumers = {};
      this.#dbClient = null;
      this.#dbOptions = dbOptions;
      this.executorEnabled = executorEnabled;
      this.#connectDB();
      this.#eventHandler();
    } else {
      this.emit("error", "Validation Error: Illegal arguments passed");
    }
  }

  #validate(args) {
    const [dbConfigs] = args;
    let validation = Object.keys(dbOptionsSample).every(
      (key) => key in dbConfigs
    );
    return validation;
  }

  #connectDB() {
    debug("Connecting to database");
    this.#dbClient = new PgClient({
      host: this.#dbOptions.DB_HOST,
      port: this.#dbOptions.DB_PORT,
      user: this.#dbOptions.DB_USER,
      password: this.#dbOptions.DB_PASSWORD,
      database: this.#dbOptions.DB_NAME,
      ssl: true,
    });

    this.#dbClient
      .connect()
      .then(() => {
        debug("Connected to PostgreSQL");
        this.emit("connect", "Connected to background server");
        createTables.call(this.#dbClient);
      })
      .catch((err) => {
        console.error("Error connecting to PostgreSQL:", err);
      });
  }

  excecutorRunning() {
    return this.#executorRunning;
  }

  consumerRunning() {
    return this.#consumerRunning;
  }

  run({ name, command, cron, schedule }) {
    if (this.executorEnabled && !this.excecutorRunning()) {
      this.#executorRunning = true;
      setInterval(() => {
        this.#jobPuller();
      }, 3000);
    }
    if (schedule) {
      insertTask.call(this.#dbClient, name, command, cron, schedule);
    }
  }

  pub({ name, message }) {
    publish.call(this.#dbClient, name, message);
  }

  #eventHandler() {
    this.on("msg-ack", ({ queue }) => {
      this.#pullMessages(queue, { prefetch: 1 });
    });
  }

  #resetInterval() {
    clearInterval(this.#queueInterval);
    this.#queueInterval = null;
  }

  #emitMessages(queue) {
    msgs = msgs.map((i) => new Message({ ...i }, this.#dbClient, this));
    this.#consumers[queue].emitter.emit("message", msgs);
  }

  async #pullMessages(queue, { prefetch }) {
    let msgs = await subscribe.call(this.#dbClient, queue, { prefetch });
    if (Boolean(msgs.length)) {
      this.#resetInterval();
      this.#emitMessages(queue);
    } else {
      if (!this.#queueInterval) {
        this.#queueInterval = setInterval(() => {
          this.#pullMessages(queue, { prefetch });
        }, 2000);
      }
    }
  }

  consumer(queue, { prefetch }) {
    let emitter = new EventEmitter();
    emitter.sub = emitter.on;
    if (!this.consumerRunning()) {
      this.#consumers[queue] = { emitter, prefetch };
      this.#consumerRunning = true;
      this.#pullMessages(queue, { prefetch });
    }
    return emitter;
  }

  async #jobPuller() {
    debug("Pulling jobs fron database");
    let messages = await fetchAndRun.call(this.#dbClient);
  }
  
}

module.exports = ZWorker;
