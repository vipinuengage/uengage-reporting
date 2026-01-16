import moment from "moment";
import { schedule } from "node-cron";
import { ensureAllConnections, prod_db_connection, report_db_connection } from "../db/mysql.js";
import { CRON_EXPRESSIONS } from "../configs/env.js";

const syncOrdersCron = () => {
  console.log("Sync Orders cron enabled.");

  let isSyncOrdersCronRunning = false;

  schedule(CRON_EXPRESSIONS.syncOrdersCronExp, async () => {
    if (isSyncOrdersCronRunning) return console.log("Previous sync orders cron still running. Skipping this cycle.");
    isSyncOrdersCronRunning = true;

    console.log("Sync orders cron started");
    console.time("Sync orders cron completed in");
    try {
      await ensureAllConnections();

      // Fetch unprocessed orders from production's metabase_order_table
      const [orders] = await prod_db_connection.execute("SELECT id, parentId, businessId, orderId, total, status, insertedAt FROM metabase_order_table WHERE status = 0 ORDER BY id ASC LIMIT 500");

      if (orders?.length) {
        let maoIds = orders.map((mao) => mao.id);
        let placeholders = maoIds.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_order_table SET status = 2 WHERE id IN (${placeholders})`, maoIds); // Marked as under-processing
      } else {
        console.log("No new orders to sync.");
        return console.timeEnd("Sync orders cron completed in");
      }

      let success_mao = [];
      let failed_mao = [];

      // Process in batches of 10
      const batchSize = 10;
      const totalBatches = Math.ceil(orders.length / batchSize);

      for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const startIndex = batchIndex * batchSize;
        const endIndex = Math.min(startIndex + batchSize, orders.length);
        const currentBatch = orders.slice(startIndex, endIndex);

        console.time(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Process each orders in the current batch in parallel
        const batchPromises = currentBatch.map(async (pOrder) => {
          try {
            const orderFields = ["orderId", "businessId", "parentId", "total", "orderInsertedAt"];
            const orderFieldsValues = orderFields.map((of) => {
              if (of == "orderInsertedAt") return pOrder["insertedAt"];
              return pOrder[of];
            });

            let [order] = await report_db_connection.execute("SELECT id FROM addo_orders WHERE orderId = ?", [pOrder.orderId]);
            let currentTime = moment().format("YYYY-MM-DD HH:mm:ss");
            if (order?.length) {
              let setFields = orderFields.map((of) => `${of} = ?`).join(",");
              await report_db_connection.execute(`UPDATE addo_orders SET ${setFields}, updatedAt = ? WHERE orderId = ?`, [...orderFieldsValues, currentTime, pOrder.orderId]);
            } else {
              let fields = orderFields.join(",");
              let placeholders = orderFields.map(() => "?").join(",");
              await report_db_connection.execute(`INSERT INTO addo_orders (${fields}, insertedAt, updatedAt) VALUES (${placeholders},?,?)`, [...orderFieldsValues, currentTime, currentTime]);
            }

            success_mao.push(pOrder?.id);
          } catch (error) {
            console.error(`Order ${pOrder?.orderId} failed: ${error.message}`);
            failed_mao.push(pOrder?.id);
          }
        });
        await Promise.all(batchPromises);

        console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Optional: Add a small delay between batches to prevent overwhelming the database
        if (batchIndex < totalBatches - 1) await new Promise((resolve) => setTimeout(resolve, 50));
      }

      if (success_mao.length) {
        let placeholders = success_mao.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_order_table SET status = 1 WHERE id IN (${placeholders})`, success_mao); // Marked as success
      }

      if (failed_mao.length) {
        let placeholders = failed_mao.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_order_table SET status = -1 WHERE id IN (${placeholders})`, failed_mao); // Marked as failed
      }

      console.timeEnd("Sync orders cron completed in");
    } catch (error) {
      console.log(`Sync orders cron error: ${error.message}`);
    } finally {
      isSyncOrdersCronRunning = false;
    }
  });
};

export { syncOrdersCron };
