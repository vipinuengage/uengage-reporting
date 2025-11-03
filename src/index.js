// import { app } from "./app.js";
import { PORT, PROD_DB_CONFIG, REPORT_DB_CONFIG } from "./configs/env.js";
import { connectProdDB, connectReportDB } from "./db/mysql.js";

import { syncBusinessCron } from "./crons/business-sync.cron.js";
import { syncOrdersCron } from "./crons/orders-sync.cron.js";
import { syncRiderLogsCron } from "./crons/rider-sync.cron.js";

(async () => {
  try {
    await connectProdDB(PROD_DB_CONFIG);
    await connectReportDB(REPORT_DB_CONFIG);

    // CRONS
    syncBusinessCron();
    syncOrdersCron();
    syncRiderLogsCron();

    // app.listen(PORT, () => console.log(`Server is running on PORT:${PORT}`));
  } catch (error) {
    console.log(`Internal Server Error:`, error);
  }
})();

