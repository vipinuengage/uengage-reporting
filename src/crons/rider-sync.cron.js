import moment from "moment";
import { schedule } from "node-cron";
import { prod_db_connection, report_db_connection } from "../db/mysql.js";
import { CRON_EXPRESSIONS } from "../configs/env.js";

const syncRiderLogsCron = () => {
    console.log("Sync self pickup rider logs cron enabled.");

    let isSyncSelfPickupRiderLogsCronRunning = false;

    // sync self pick riders
    schedule(CRON_EXPRESSIONS.syncRiderLogsCronExp, async () => {
        if (isSyncSelfPickupRiderLogsCronRunning) {
            return console.log("⏸ Previous sync still running, skipping this cycle.");
        }

        isSyncSelfPickupRiderLogsCronRunning = true;
        console.log("🚀 Sync self pickup rider logs cron started");
        console.time("Sync self pickup rider logs cron completed in");

        try {
            // Fetch last inserted timestamp from report DB
            const [lastLogRows] = await report_db_connection.execute(`
                SELECT MAX(insertedAt) AS lastInsertedAt 
                FROM rider_logs
            `);

            const lastInsertedAt = lastLogRows[0]?.lastInsertedAt;
            const todayStart = moment().startOf("day");
            const todayStartStr = todayStart.format("YYYY-MM-DD HH:mm:ss");

            const startTimestamp = !lastInsertedAt || moment(lastInsertedAt).isBefore(todayStart)
                ? todayStartStr
                : moment(lastInsertedAt).format("YYYY-MM-DD HH:mm:ss");

            // Fetch new data from prod DB
            const [flash_mis] = await prod_db_connection.execute(`
                SELECT 
                    fm.*,
                    fm.prepaid_count as prepaidCount, 
                    fm.cod_count as codCount, 
                    ab.parentId AS parentId, 
                    2 AS orderType
                FROM flash_mis fm
                LEFT JOIN addo_business ab ON ab.id = fm.businessId
                WHERE 
                    fm.insertedAt > ?
                    AND ab.parentId = 5
                ORDER BY fm.id DESC;
            `, [startTimestamp]);

            if (!flash_mis?.length) {
                console.log("ℹ️ No new self pickup rider logs to sync.");
                console.timeEnd("Sync self pickup rider logs cron completed in");
                return;
            }

            // Batch process
            const batchSize = 50;
            const totalBatches = Math.ceil(flash_mis.length / batchSize);

            for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
                const startIndex = batchIndex * batchSize;
                const endIndex = Math.min(startIndex + batchSize, flash_mis.length);
                const currentBatch = flash_mis.slice(startIndex, endIndex);

                console.time(`Processing batch ${batchIndex + 1}/${totalBatches} (${currentBatch.length} items)`);

                const batchPromises = currentBatch.map(async (fmi) => {
                    try {
                        const fields = ["businessId", "parentId", "orderType", "prepaidCount", "codCount", "insertedAt"];
                        const values = fields.map((field) => fmi[field]);

                        const placeholders = fields.map(() => "?").join(",");
                        await report_db_connection.execute(
                            `INSERT INTO rider_logs (${fields.join(",")}) VALUES (${placeholders})`,
                            values
                        );
                    } catch (err) {
                        console.error(`❌ Failed to insert rider log ID ${fmi?.id}: ${err.message}`);
                    }
                });

                await Promise.all(batchPromises);

                console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} (${currentBatch.length} items)`);

                // Small delay between batches
                if (batchIndex < totalBatches - 1) {
                    await new Promise((resolve) => setTimeout(resolve, 50));
                }
            }

            console.timeEnd("Sync self pickup rider logs cron completed in");
        } catch (err) {
            console.error(`🔥 Sync self pickup rider logs cron error: ${err.message}`);
        } finally {
            isSyncSelfPickupRiderLogsCronRunning = false;
        }
    });
};

export { syncRiderLogsCron };
