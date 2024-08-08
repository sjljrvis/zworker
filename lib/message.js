const debug = require("debug")("zworker:messages");
const { markAck } = require("./database");

class Message {
  #db;
  #parent;
  constructor(data, dbConn, parent) {
    this.data = data;
    this.#db = dbConn;
    this.#parent = parent;
  }

  ack() {
    debug("acking msg");
    markAck.call(this.#db, this.data.name ,[this.data.id]);
    this.#parent.emit("msg-ack", { queue: this.data.name });
  }
}

module.exports = Message;
