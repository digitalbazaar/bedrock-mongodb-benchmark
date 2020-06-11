/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
import bedrock from 'bedrock';
import * as database from 'bedrock-mongodb';
import pMap from 'p-map';
import {promisify} from 'util';
const {util: {delay, uuid, BedrockError}} = bedrock;

import './config.js';

const BATCH_SIZE = 1000;
const COUNT_INTERVAL = 10000;
const CONCURRENCY = 100;

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await promisify(database.openCollections)(['benchmark-test']);

  await promisify(database.createIndexes)([{
    collection: 'benchmark-test',
    fields: {'data.id': 1},
    options: {unique: true, background: false}
  }, {
    collection: 'benchmark-test',
    fields: {'data.notUnique': 1},
    options: {unique: false, background: false}
  }]);
});

bedrock.events.on('bedrock.start', async () => {
  // register counter to run only on one worker
  bedrock.runOnce('count-records', async () => {
    let lastCount = 0;
    let lastTimeStamp = 0;
    while(true) {
      const now = Date.now();
      const currentCount = await count();
      const newCount = currentCount - lastCount;

      const intervalSeconds = (now - lastTimeStamp) / 1000;
      const recordsPerSecond = Math.floor(newCount / intervalSeconds);
      console.log(`==== ${now} ===============================`);
      console.log('CURRENT COUNT', currentCount);
      console.log('NEW COUNT', newCount);
      console.log('RECORDS PER SECOND:', recordsPerSecond);
      lastCount = currentCount;
      lastTimeStamp = now;
      await delay(COUNT_INTERVAL);
    }
  });

  while(true) {
    const records = [];
    for(let i = 0; i < BATCH_SIZE; ++i) {
      records.push({data: {
        id: uuid(),
        notUnique: uuid(),
      }});
    }
    await pMap(records, r => insert(r), {concurrency: CONCURRENCY});
  }
});

export async function insert({data}) {
  const collection = database.collections['benchmark-test'];
  const now = Date.now();
  const meta = {created: now, updated: now};
  const record = {
    meta,
    data
  };
  try {
    await collection.insert(record, database.writeOptions);
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
  if(!(id && notUnique)) {
    throw new TypeError('Either "id" or "notUnique" must be given.');
  }
  if(id) {
    query['data.id'] = id;
  }
  if(notUnique) {
    query['data.notUnique'] = notUnique;
  }
  const projection = {_id: 0};
  const collection = database.collections['benchmark-test'];
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
  const collection = database.collections['benchmark-test'];
  return collection.countDocuments({});
}
