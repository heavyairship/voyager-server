import * as express from 'express';
import {fetchCompassQLBuildSchema, fetchCompassQLRecommend, Schema} from 'datavoyager/build/src/api/api';
import {Data} from 'vega-lite/build/src/data';
import {serializeSchema} from './utils';

const router = express.Router();

// PostgreSQL client initialization
const pg = require('pg');
const pghost = 'localhost';
const pgport = '5432';
const pgdb = 'voyager';
const connectionString = 'postgres://' + pghost + ':' + pgport  + '/' + pgdb;
console.log('Connecting to '+connectionString);
const client = new pg.Client(connectionString);
client.connect();
var tableNameCounter = 0;

function postgresTypeFor(value : any): string {
  // FixMe: want to use INTs too, if possible. Client needs to send more data.
  const type = typeof value;
  if(type === 'string') {
    return 'VARCHAR(128)';
  } else if(type === 'number') {
    return 'FLOAT';
  } else if(type === 'boolean') {
    return 'BOOLEAN';
  } else {
    console.log('ERROR: undefined type: \'' + type + '\'');
  }
}

function postgresSchemaFor(dataObj: any): string {
  let schema: any = {}
  for(var property in dataObj) {
    if(dataObj.hasOwnProperty(property)) {
      let pgType = postgresTypeFor(dataObj[property]);
      schema[property] = postgresTypeFor(dataObj[property]);
    }
  }
  return schema;
}

function listToSQLTuple(l: any[], keepQuotes: boolean): string {
  let out: string = JSON.stringify(l);
  out = out.substring(1, out.length - 1);
  out = out.replace(/'/g, '\'\'');
  out = out.replace(/"/g, keepQuotes? '\'' : '');
  return out;
}

function createInsertQueryStrFor(tableName: string, schema: any, data: any): string {
  let attrNames: string[] = [];
  for(var attrName in schema) {
    if(!schema.hasOwnProperty(attrName)) {
      continue;
    }
    attrNames.push(attrName);
  }
  let attrNamesStr = listToSQLTuple(attrNames, false);

  let attrVals: any[] = [];
  for(let i: number = 0; i < attrNames.length; i++) {
    attrVals.push(data[attrNames[i]]);
  }
  let attrValsStr = listToSQLTuple(attrVals, true);

  let out: string = 'INSERT INTO ' + tableName + '(' +attrNamesStr + ') VALUES (' + attrValsStr + ');';
  return out;
}

function insertValues(tableName: string, data: any[], res: express.Response): void {
  const schema = postgresSchemaFor(data[0]);
  for(let i: number = 0; i < data.length; i++) {
    let insertQueryStr: string = createInsertQueryStrFor(tableName, schema, data[i]);
    if(i === 0) {
      console.log('INFO: running insert queries. Example: ' + insertQueryStr);
    }
    const insertQuery = client.query(insertQueryStr, (err: any, response: any) => {
      if(err) {
        console.log(err);
      }
      if(i === (data.length-1)) {
        // FixMe: I don't think this is strictly correct. We don't
        // want to set the status until all the queries have completed.
        // Ideally, we will batch all the insert queries into a single one
        // and add this in the call back to that single batched query. 
        // We can use pg-format or node-sql for this.
        res.status(200).send();
      }
    });
  }
}

function createTableQueryStrFor(tableName: string, schema: any): string {
  let out: string = 'CREATE TABLE ' + tableName + '('
  let first: boolean = true;
  for(var attrName in schema) {
    if(!schema.hasOwnProperty(attrName)) {
      continue;
    }
    let attrType: string = schema[attrName];
    if(first) {
      first = false;
    } else {
      out += ', ';
    }
    out += (attrName + ' ' + attrType)
  }
  out += ');';
  return out;
}

/**
 * Root Route
 * Just returns dummy JSON to indicate server is responsive
 */
router.route('/').get((req: express.Request, res: express.Response) => {
  res.status(200).send({ready: true});
});

/**
 * recommend route
 * Returns results from fetchCompassQLRecommend in serialized JSON.
 */
router.route('/recommend').post((req: express.Request, res: express.Response) => {
  const query = req.body.query;
  const fieldSchemas = req.body.schema;
  const data = (req.body.data as Data);
  const schema: Schema = new Schema({fields: fieldSchemas});
  fetchCompassQLRecommend(query, schema, data).then(
    result => {
      res.status(200).send(result);
    }
  );
});

/**
 * insertSQL route
 * Inserts data into SQL table.
 */
router.route('/insertSql').post((req: express.Request, res: express.Response) => {
  // FixMe: if there are network issues, it is possible for this 
  // request to be received. This will result in duplicate entries,
  // which is bad. We need a way to identify requests so that we can
  // not process duplicates. 
  const chunk = req.body.data;
  if(chunk.length === 0) {
    console.log('WARNING: empty data passed to /insertSql');
    res.status(200).send();
    return;
  }
  insertValues(req.body.name, chunk, res);
});

/**
 * checkExistsSql route
 * Check if PostgreSQL table exists.
 */
router.route('/checkExistsSql').post((req: express.Request, res: express.Response) => {
  const sample = req.body.data;
  const name = req.body.name;
  const existsQueryStr = 'SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='+
    '\'' + name.toLowerCase() + '\');'
  client.query(existsQueryStr, (err: any, response: any) => {
    if(err) {
      // Exists query failed
      console.log(err);
      res.status(500).send(err);
    } else if(response.rows[0]['exists']) {
      // Table exists
      res.status(200).send({exists: true});
      console.log('INFO: table ' + name + ' already exists.');
    } else {
      // Table doesn't exist
      console.log('INFO: table ' + name + ' does not exist');
      res.status(200).send({exists: false});
    }
  });
});

/**
 * createSQL route
 * Creates SQL table using schema inferred from data sample.
 */
router.route('/createSql').post((req: express.Request, res: express.Response) => {
  const sample = req.body.data;
  const name = req.body.name;
  console.log('INFO: creating table ' + name);
  const schema = postgresSchemaFor(sample);
  console.log('INFO: built postgres schema: '+JSON.stringify(schema));
  const createTableQueryStr = createTableQueryStrFor(name, schema);
  console.log('INFO: running create query: ' + createTableQueryStr);  
  client.query(createTableQueryStr, (err: any, response: any) => {
    if(err) {
      // Table create query failure
      console.log(err);
      res.status(500).send(err);
    } else {
      // Table create query success
      res.status(200).send();
    }
  });
});
 
/**
 * build route
 * Returns from fetchCompassQLBuildSchema in serialzied JSON.
 */
router.route('/build').post((req: express.Request, res: express.Response) => {
  const name = req.body.name;
  const queryStr = 'SELECT * FROM ' + name + ';';
  console.log('INFO: running query for /build: ' + queryStr);
  const query = client.query(queryStr, (err: any, data: any) => {
    if(err) {
      console.log(err);
      res.status(500).send(err);
    } else {
      fetchCompassQLBuildSchema(data.rows).then(result => {
        res.status(200).send(serializeSchema(result));
      });
    }
  }); 
});

/**
 * querySql route
 * Returns results of query in serialzied JSON.
 */
router.route('/querySql').post((req: express.Request, res: express.Response) => {
  const queryStr = req.body.data['query'];
  console.log('INFO: running query for /querySql: ' + queryStr);
  const query = client.query(queryStr, (err: any, results: any) => {
    if(err) {
      console.log(err);
      res.status(500).send(err);
    } else {
      res.status(200).send(results); 
    }
  }); 
});

export = router;
