import mysql from "mysql2/promise";

let prod_db_connection;
let report_db_connection;

const connectProdDB = async (config) => {
  try {
    prod_db_connection = await mysql.createConnection(config);
  } catch (error) {
    throw new Error(`Error connecting PROD DB: ${error.message}`);
  }
};
const connectReportDB = async (config) => {
  try {
    report_db_connection = await mysql.createConnection(config);
  } catch (error) {
    throw new Error(`Error connecting Repot DB: ${error.message}`);
  }
};

export { prod_db_connection, report_db_connection, connectProdDB, connectReportDB };
