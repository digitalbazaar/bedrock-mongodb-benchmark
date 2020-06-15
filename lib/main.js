/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
import bedrock from 'bedrock';
import * as database from 'bedrock-mongodb';
import pMap from 'p-map';
import {promisify} from 'util';
const {util: {delay, uuid, BedrockError}} = bedrock;

import './config.js';

const READ_SAMPLE_SIZE = 10000;
const WRITE_BATCH_SIZE = 1000;
const READ_BATCH_SIZE = 1000;
const STATS_DELAY_MS = 10000;
const WRITE_CONCURRENCY = 25;
const READ_CONCURRENCY = 25;

bedrock.events.on('bedrock-cli.init', () => bedrock.program.option(
  '--bench-mode <mode>',
  'Mode for benchmark: read, write, both'
));

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await promisify(database.openCollections)(['benchmark']);

  await promisify(database.createIndexes)([{
    collection: 'benchmark',
    fields: {'data.id': 1},
    options: {unique: true, background: false}
  }, {
    collection: 'benchmark',
    fields: {'data.notUnique': 1},
    options: {unique: false, background: false}
  }]);
});

bedrock.events.on('bedrock.start', async () => {
  // register counter to run only on one worker
  const {program: {benchMode}} = bedrock;
  if(benchMode === 'write') {
    setupWriteCounter();
  }
  if(benchMode === 'read') {
    setupReadCounter();
  }

  // benchMode can be read/write/both
  if(benchMode === 'write') {
    while(true) {
      await writeBatch();
    }
  } else if(benchMode === 'read') {
    console.log(`Clearing all records in preparation for read test...`);
    const collection = database.collections['benchmark'];
    await collection.deleteMany({});

    console.log(`Creating ${READ_SAMPLE_SIZE} records...`);
    const records = [];
    // create records until the sample set size has been created
    while(await count() < READ_SAMPLE_SIZE) {
      records.push(...await writeBatch());
    }
    console.log(`Created ${READ_SAMPLE_SIZE} records.`);

    // read records
    console.log(`Performing read tests...`);
    while(true) {
      const count = await readBatch(records);
      bedrock.events.emit('benchmark.readBatchComplete', {count});
    }
  } else {
    console.log('Unknown benchmark mode:', benchMode);
    console.log(`Connected to database, found ${await count()} records.`);
  }
});

export async function insert({data}) {
  const collection = database.collections['benchmark'];
  const now = Date.now();
  const meta = {created: now, updated: now};
  const record = {
    meta,
    data
  };
  try {
    await collection.insertOne(record, database.writeOptions);
  } catch(e) {
    if(!database.isDuplicateError(e)) {
      throw e;
    }
    throw new BedrockError(
      'Duplicate record.',
      'DuplicateError', {
        public: true,
        httpStatusCode: 409
      }, e);
  }
}

export async function get({id, notUnique}) {
  const query = {};
  if(!(id || notUnique)) {
    throw new TypeError('Either "id" or "notUnique" must be given.');
  }
  if(id) {
    query['data.id'] = id;
  }
  if(notUnique) {
    query['data.notUnique'] = notUnique;
  }
  const projection = {_id: 0};
  const collection = database.collections['benchmark'];
  const record = await collection.findOne(query, projection);
  if(!record) {
    const details = {
      httpStatusCode: 404,
      public: true
    };
    throw new BedrockError(
      'Record not found.',
      'NotFoundError', details);
  }

  return record;
}

export async function count() {
  const collection = database.collections['benchmark'];
  return collection.countDocuments({});
}

async function setupWriteCounter() {
  bedrock.runOnce('count-writes', async () => {
    let lastCount = 0;
    let lastTimeStamp = 0;
    while(true) {
      const now = Date.now();
      const currentCount = await count();
      const newCount = currentCount - lastCount;

      const intervalSeconds = (now - lastTimeStamp) / 1000;
      const recordsPerSecond = Math.floor(newCount / intervalSeconds);
      console.log(`--------------------- ${Date().toLocaleString()}`);
      console.log('    Total Records:', currentCount);
      console.log('      New Records:', newCount);
      console.log('Writes per Second:', recordsPerSecond);
      lastCount = currentCount;
      lastTimeStamp = now;
      await delay(STATS_DELAY_MS);
    }
  });
}

async function writeBatch() {
  const records = [];
  for(let i = 0; i < WRITE_BATCH_SIZE; ++i) {
    records.push({data: {
      id: uuid(),
      notUnique: uuid(),
    }});
  }
  await pMap(records, r => insert(r), {concurrency: WRITE_CONCURRENCY});

  return records;
}

async function readBatch(records) {
  const ids = [];
  for(let i = 0; i < READ_BATCH_SIZE; ++i) {
    ids.push(records[Math.floor(Math.random() * records.length)].data.id);
  }
  await pMap(ids, id => get({id}), {concurrency: READ_CONCURRENCY});

  return READ_BATCH_SIZE;
}

async function setupReadCounter() {
  bedrock.runOnce('count-reads', async () => {
    let totalReads = 0;
    let lastCount = 0;
    let lastTimeStamp = 0;
    bedrock.events.on('benchmark.readBatchComplete', async readStats => {
      totalReads += readStats.count;
      return;
    });

    while(true) {
      const now = Date.now();
      const currentCount = totalReads;
      const newCount = currentCount - lastCount;
      const intervalSeconds = (now - lastTimeStamp) / 1000;
      const recordsPerSecond = Math.floor(newCount / intervalSeconds);
      console.log(`--------------------- ${Date().toLocaleString()}`);
      console.log('     Total Reads:', currentCount);
      console.log('       New Reads:', newCount);
      console.log('Reads per Second:', recordsPerSecond);
      lastCount = totalReads;
      lastTimeStamp = now;
      await delay(STATS_DELAY_MS);
    }
  });
}
