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

const CRON_EXPRESSIONS = {
  syncOrdersCronExp: process.env.SYNC_ORDERS_CRON_EXP,
  syncBusinessCronExp: process.env.SYNC_BUSINESS_CRON_EXP,
  syncBusinessMenuScoreCronExp: process.env.SYNC_BUSINESS_MENUSCORE_CRON_EXP,
  syncBusinessOrderTypeCronExp: process.env.SYNC_BUSINESS_ORDERTYPE_CRON_EXP,
  syncBusinessReviewsCronExp: process.env.SYNC_BUSINESS_REVIEWS_CRON_EXP,
  syncRiderLogsCronExp: process.env.SYNC_RIDER_LOGS_CRON_EXP
}

export { PORT, PROD_DB_CONFIG, REPORT_DB_CONFIG, CRON_EXPRESSIONS };
