import { config } from "dotenv";
config({ quiet: true });

const PORT = process.env.PORT || 8000;

const PROD_DB_CONFIG = {
  host: process.env.PROD_DB_HOST,
  user: process.env.PROD_DB_USER,
  password: process.env.PROD_DB_PASS,
  database: process.env.PROD_DB_NAME,
};

const REPORT_DB_CONFIG = {
  host: process.env.REPORT_DB_HOST,
  user: process.env.REPORT_DB_USER,
  password: process.env.REPORT_DB_PASS,
  database: process.env.REPORT_DB_NAME,
};

export { PORT, PROD_DB_CONFIG, REPORT_DB_CONFIG };
