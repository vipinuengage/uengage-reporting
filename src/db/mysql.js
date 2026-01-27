import mysql from "mysql2/promise";

let prod_db_connection;
let report_db_connection;

let prodConfig;
let reportConfig;

const connectProdDB = async (config) => {
  prodConfig = config;
  prod_db_connection = await mysql.createPool(config);
};

const connectReportDB = async (config) => {
  reportConfig = config;
  report_db_connection = await mysql.createPool(config);
};

const ensureProdConnection = async () => {
  try {
    if (!prod_db_connection || prod_db_connection.connection.state === "disconnected") {
      console.log("Reconnecting PROD DB...");
      prod_db_connection = await mysql.createPool(prodConfig);
    }
  } catch (err) {
    console.error("PROD DB reconnect failed:", err.message);
    throw err;
  }
};

const ensureReportConnection = async () => {
  try {
    if (!report_db_connection || report_db_connection.connection.state === "disconnected") {
      console.log("Reconnecting REPORT DB...");
      report_db_connection = await mysql.createPool(reportConfig);
    }
  } catch (err) {
    console.error("REPORT DB reconnect failed:", err.message);
    throw err;
  }
};

const ensureAllConnections = async () => {
  await ensureProdConnection();
  await ensureReportConnection();
};

export { prod_db_connection, report_db_connection, connectProdDB, connectReportDB, ensureProdConnection, ensureReportConnection, ensureAllConnections };
