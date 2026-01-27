import mysql from "mysql2/promise";

let prod_db_connection;
let report_db_connection;

const connectProdDB = async (config) => {
  if (!prod_db_connection) prod_db_connection = await mysql.createPool(config);
  return prod_db_connection;
};

const connectReportDB = async (config) => {
  if (!report_db_connection) report_db_connection = await mysql.createPool(config);
  return report_db_connection;
};

const ensureAllConnections = async () => {
  console.log("...");
};

export { prod_db_connection, report_db_connection, connectProdDB, connectReportDB, ensureAllConnections };
