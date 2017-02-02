'use strict';

const autoMap = require('automap');
const glob = require('glob');
const yargs = require('yargs');
const fs = require('fs');
const elbParser = require('elb-log-parser');
const mongodb = require('mongodb');
const Logr = require('logr');
const log = new Logr({
  type: 'cli'
});

const argv = yargs
  .option('logs', {
    describe: 'logs to parse and import',
    default: false,
    type: 'string'
  })
  .option('host', {
    describe: 'mongo host',
    default: 'localhost',
    type: 'string'
  })
  .option('port', {
    describe: 'mongo port',
    default: '27017',
    type: 'string'
  })
  .option('database', {
    describe: 'mongo database',
    default: 'elblogs',
    type: 'string'
  })
  .option('collection', {
    describe: 'mongo collection',
    default: 'logs',
    type: 'string'
  })
  .help('h')
  .argv;


mongodb.MongoClient.connect(`mongodb://${argv.host}:${argv.port}/${argv.database}`, (connErr, db) => {
  if (connErr) {
    throw connErr;
  }
  autoMap(
    (done) => {
      glob(argv.logs, done);
    },
    5,
    {
      log(item, itemIndex, done) {
        log(`${itemIndex}. Parsing: ${item}`);
        done();
      },
      contents(itemIndex, item, done) {
        log(`${itemIndex}. Reading File`);
        fs.readFile(item, 'utf8', done);
      },
      lines(itemIndex, contents, done) {
        log(`${itemIndex}. Splitting File`);
        done(null, contents.split('\n'));
      },
      json(itemIndex, lines, done) {
        log(`${itemIndex}. Parsing File`);
        const json = lines.map((line) => {
          if (!line) {
            return {};
          }
          return elbParser(line);
        });
        done(null, json);
      },
      insert(itemIndex, json, done) {
        log(`${itemIndex}. Inserting into mongo`);
        const logs = db.collection(argv.collection);
        logs.insertMany(json, done);
      },
      doneLog(item, itemIndex, itemCount, insert, done) {
        log(`${itemIndex}/${itemCount}. Finished Parsing: ${item}`);
        done();
      }
    },
    (item, results) => (item),
    (err, results) => {
      if (err) {
        throw err;
      }
      db.close();
      log(results);
    });

});
