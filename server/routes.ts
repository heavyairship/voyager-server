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
  console.log('INFO: built postgres schema: '+JSON.stringify(schema));
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

function handleInsertQuery(err: any, res: any, queryStr: string, data: any): void {
  if(err) {
    console.log(err);
    console.log('Query string: ' + queryStr);
    console.log('Data: ' + JSON.stringify(data));
  }
}

function insertValues(tableName: string, schema: any, data: any[]): void {
  for(let i: number = 0; i < data.length; i++) {
    let insertQueryStr: string = createInsertQueryStrFor(tableName, schema, data[i]);
    if(i === 0) {
      console.log('INFO: running insert queries. Example: ' + insertQueryStr);
    }
    const insertQuery = client.query(insertQueryStr,
      (err: any, res: any) => {handleInsertQuery(err, res, insertQueryStr, data[i])});
  }
}

function handleCreateTableQuery(err: any, res: any, tableName: string, schema:any, data: any[]): void {
  if(err) {
    console.log(err);
  } else {
    insertValues(tableName, schema, data); 
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
  out += ')';
  return out;
}

function handleTableExistsQuery(err: any, res: any, tableName: string, data: any[]): void {
  const schema = postgresSchemaFor(data[0]);
  if(err) {
    console.log(err);
  } else if(res.rows[0]['exists']) {
    console.log('WARNING: table \'' + tableName + 
      ' \'already exists. Performing bag-semantics update');
    insertValues(tableName, schema, data);
  } else {
    console.log('INFO: table ' + tableName + ' does not exist -- creating');
    const createTableQueryStr = createTableQueryStrFor(tableName, schema);
    console.log('INFO: running create query: ' + createTableQueryStr);  
    const createTableQuery = client.query(createTableQueryStr,
      (err: any, res: any) => {handleCreateTableQuery(err, res, tableName, schema, data)});
  }
}

function createTable(data: any[], tableName: string): any {
  // Check if table exists
  // Note: lower-case is required to prevent the exists query from generating false negatives.
  // If 'Cars' is passed in as a name initially, 'cars' will be stored in postgres' 
  // information_schema. Then, if 'Cars' is passed in again, the exists query will erroneously 
  // report false since 'Cars' != 'cars', even though both names refer to the same table in 
  // postgres. To prevent this, we just change table names to all lower-case before the exists
  // check query.
  const existsQueryStr = 'SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='+
    '\'' + tableName.toLowerCase() + '\');'
  const existsQuery = client.query(existsQueryStr, 
    (err: any, res: any) => {handleTableExistsQuery(err, res, tableName, data)});
  return existsQuery;
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
 * createSQL route
 * Builds sql table from dataset.
 */
router.route('/createSql').post((req: express.Request, res: express.Response) => {
  // FixMe: need to use promises here for better organization/error handling.
  const data = req.body.data;
  const name = req.body.name;
  if(data.length !== 0) {
    createTable(data, name).then(() => res.status(200).send());
  } else {
    console.log('WARNING: data len is 0, could not build schema or create table');
    res.status(200).send();
  }
});
 
/**
 * build route
 * Returns from fetchCompassQLBuildSchema in serialzied JSON.
 */
router.route('/build').post((req: express.Request, res: express.Response) => {
  const name = req.body.name;
  const queryStr = 'SELECT * FROM ' + name + ';';
  console.log('INFO: running query for /build: ' + queryStr);
  const query = client.query(queryStr, 
    (err: any, data: any) => {
      if(err) {
        console.log(err);
        res.status(500).send(err);
      } else {
        fetchCompassQLBuildSchema(data.rows).then(
          result => {
            res.status(200).send(serializeSchema(result));
          }
        );
      }
    }
  ); 

});

/**
 * query route
 * Returns results of query in serialzied JSON.
 */
router.route('/query').post((req: express.Request, res: express.Response) => {
  const queryStr = req.body.data['query'];
  console.log('INFO: running query for /query: ' + queryStr);
  const query = client.query(queryStr, 
    (err: any, results: any) => {
      if(err) {
        console.log(err);
        res.status(500).send(err);
      } else {
        res.status(200).send(results); 
      }
    }
  ); 
});

export = router;
