const debug = require("debug")("zworker:db");

module.exports.createTables = function () {
  try {
    debug("creating DB tables if not present");
    let create_task_table_query = `CREATE TABLE IF NOT EXISTS zombie_tasks (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      command TEXT,
      cron VARCHAR(255),
      schedule TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`;

    let create_jobs_table_query = `CREATE TABLE IF NOT EXISTS zombie_jobs (
      id SERIAL PRIMARY KEY,
      task_id INT,
      status VARCHAR(255) NOT NULL,
      scheduled_at TIMESTAMP,
      picked_at TIMESTAMP,
      completed_at TIMESTAMP,
      failed_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (task_id) REFERENCES zombie_tasks(id) ON DELETE CASCADE ON UPDATE CASCADE
    );`;


    let create_queue_table_query = `CREATE TABLE IF NOT EXISTS zombie_queues (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      status VARCHAR(255) NOT NULL,
      message TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`;

    this.query(create_task_table_query);
    this.query(create_jobs_table_query);
    this.query(create_queue_table_query);
  } catch (e) {
    debug("Error creating table %s", e.toString());
  }
};

module.exports.insertTask = async function (name, command, cron, schedule) {
  try {
    debug("inserting data in job DB");
    let create_task_query = `INSERT INTO zombie_tasks (name, command, cron, schedule)
    VALUES ($1, $2, $3, $4) RETURNING *`;
    debug('Running [SQL] : %s', create_task_query)
    let {rows: [data] } = await this.query(create_task_query, [name, command, cron, schedule]);
    let job = {
      task_id : data.id,
      status: 'queued',
      scheduled_at: data.schedule
    }
    insertJobs.call( this, job.task_id,job.status,job.scheduled_at)
  } catch (e) {
    debug("Error creating entry %s", e.toString());
  }
};

const insertJobs = async function (task_id, status, scheduled_at) {
  try {
    debug("inserting data in job DB");
    let create_job_query = `INSERT INTO zombie_jobs (task_id, status, scheduled_at)
    VALUES ($1, $2, $3) RETURNING *`;
    debug('Running [SQL] : %s', create_job_query)
    let {rows: [data]} = await this.query(create_job_query, [task_id, status, scheduled_at]);
  } catch (e) {
    debug("Error creating entry %s", e.toString());
  }
};


module.exports.fetchAndRun = async function () {
  try {
    debug("acquiring lock & fetch");
    let msgs = []
    let now = new Date()
    let begin_query = 'BEGIN'
    let commit_query = 'COMMIT'
    let select_jobs_query = `SELECT * FROM zombie_jobs 
                             WHERE status = 'queued'
                             FOR UPDATE SKIP LOCKED
                             LIMIT 1
                            `
    let update_jobs_query = `UPDATE zombie_jobs
                             SET status = 'processing'
                             WHERE id = $1
                            `

    await this.query(begin_query)
    let {rows: [data]} = await this.query(select_jobs_query)
    if(data) {
      msgs = [data]
      await this.query(update_jobs_query, [data.id])
    } else {
      debug('Processed all messages')
    }
    await this.query(commit_query)
    return msgs
  } catch (e) {
    console.log(e)
    this.query('ROLLBACK')
    debug("Error creating entry %s", e.toString());
  }
};


module.exports.publish = async function(name,message) {
  try {
    debug("inserting data in queue DB");
    let create_msg_query = `INSERT INTO zombie_queues (name, status, message)
    VALUES ($1, $2, $3) RETURNING *`;
    let {rows: [data]} = await this.query(create_msg_query, [name, 'ready', message]);
  } catch(e) {
    debug("Error creating entry %s", e.toString());
  }
}

module.exports.subscribe = async function(name, {prefetch}) {
  try {
    debug("acquiring lock & fetch");
    let msgs = []
    let now = new Date()
    let begin_query = 'BEGIN'
    let commit_query = 'COMMIT'
    let select_msg_query = `SELECT * FROM zombie_queues
                             WHERE status = 'ready'
                             and name = '${name}'
                             FOR UPDATE SKIP LOCKED
                             LIMIT ${prefetch}
                            `
    let update_msg_query = `UPDATE zombie_queues
                             SET status = 'delivered'
                             WHERE id = ANY($1::int[])
                            `

    await this.query(begin_query)
    let {rows: data} = await this.query(select_msg_query)
    if(data) {
      msgs = data
      // await this.query(update_msg_query, [msgs.map(i=> i.id)])
    } else {
      debug('Processed all messages')
    }
    await this.query(commit_query)
    return msgs
  } catch (e) {
    console.log(e)
    this.query('ROLLBACK')
    debug("Error creating entry %s", e.toString());
  }
}

module.exports.markAck = async function(name, ids) {
  try {
    let begin_query = 'BEGIN'
    let commit_query = 'COMMIT'

    let update_msg_query = `UPDATE zombie_queues
                             SET status = 'delivered'
                             WHERE 
                             name = '${name}'
                             and id = ANY($1::int[])`

    await this.query(begin_query)
    await this.query(update_msg_query, [ids])
    await this.query(commit_query)
  } catch(e) {
    console.log(e)
    this.query('ROLLBACK')
    debug("Error creating entry %s", e.toString());
  }
}