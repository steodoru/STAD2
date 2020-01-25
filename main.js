"use strict";

const config  = require('./config.json'); 

const rfcClient = require('node-rfc').Client;
const {BigQuery} = require('@google-cloud/bigquery');

const date_ob = new Date();
date_ob.setHours(date_ob.getHours() - 2);
let start_date = date_ob.getUTCFullYear() + ("0" + (date_ob.getUTCMonth() + 1)).slice(-2) + ("0" + date_ob.getUTCDate()).slice(-2);
let start_hour = date_ob.getUTCHours();
date_ob.setHours(date_ob.getHours() + 1);
let end_date = date_ob.getUTCFullYear() + ("0" + (date_ob.getUTCMonth() + 1)).slice(-2) + ("0" + date_ob.getUTCDate()).slice(-2);
//let end_hour = date_ob.getUTCHours();

async function main() {
  const SAPclient = new rfcClient(config.SAP_AS);
    try {
      await SAPclient.open();
      let data = [];
      let params =  {
        READ_START_DATE: start_date,
        READ_START_TIME: start_hour + "0000",
        READ_END_DATE: end_date,
        READ_END_TIME: start_hour + "5959",
        };
      console.log(params);
      let result = await SAPclient.call('SWNC_STATREC_READ_INSTANCE', params);
      var statrecs = result['STATRECS'];
      for (const element of statrecs) data.push(element['MAINREC']);
      insertRowsAsStream(data);
      }
      catch (ex) {
      console.error(ex);
      };
    SAPclient.close();
  };

async function insertRowsAsStream(rows) {
  const bigquery = new BigQuery(config.BQ_options);
  try {
    await bigquery.dataset('STAD').table('STAD').insert(rows);
    console.log(`Inserted ${rows.length} rows`);
  }
  catch (ex) {
  var err = ex['errors']
  console.log(err)
  }
}

main();