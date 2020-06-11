/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
import {config} from 'bedrock';

config.core.workers = 0;

config.mongodb.name = 'bedrock_mongodb_benchmark_localhost';
config.mongodb.host = 'localhost';
config.mongodb.port = 27017;
// config.mongodb.username = 'bedrock';
// config.mongodb.password = 'password';
