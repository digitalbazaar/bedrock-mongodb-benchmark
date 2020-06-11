/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
import * as database from 'bedrock-mongodb';
import {promisify} from 'util';

import './config.js';

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

export async function insert({data}) {
  const collection = database.collections['benchmark-test'];
  const now = Date.now();
  const meta = {created: now, updated: now};
  let record = {
    meta,
    data
  };
  try {
    const result = await collection.insert(record, database.writeOptions);
    record = result.ops[0];
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

// TODO: export anything else
