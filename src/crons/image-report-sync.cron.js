import moment from "moment";
import { schedule } from "node-cron";
import { prod_db_connection, report_db_connection } from "../db/mysql.js";
import { CRON_EXPRESSIONS } from "../configs/env.js";

const syncImageLogsCron = async () => {
    console.log("Sync image logs cron enabled.");

    let isSyncImageLogsCronRunning = false;

    // sync image logs
    schedule(CRON_EXPRESSIONS.syncImageLogCronExp, async () => {
        if (isSyncImageLogsCronRunning) return console.log("⏸ Previous sync still running, skipping this cycle.");
        isSyncImageLogsCronRunning = true;

        console.log("🚀 Sync image logs cron started");
        console.time("Sync image logs cron completed in");
        try {
            // Fetch last inserted timestamp from report DB
            const [lastLogRows] = await report_db_connection.execute("SELECT refId from image_logs order by id desc limit 1");
            const lastRefId = lastLogRows[0]?.refId;

            let prod_image_logs_query = "SELECT * FROM image_logs WHERE parentBusinessId = 5";
            const params = [];
            if (lastRefId) {
                prod_image_logs_query += " AND id > ?";
                params.push(lastRefId);
            }
            prod_image_logs_query += " ORDER BY id ASC LIMIT 1000";

            const [prod_image_logs] = await prod_db_connection.execute(prod_image_logs_query, params);


            for (const pil of prod_image_logs) {
                try {
                    let createdAt = moment(pil?.createdAt).format("YYYY-MM-DD HH:mm:ss")
                    let currentTimestamp = moment().format("YYYY-MM-DD HH:mm:ss");
                    const { id, parentBusinessId, businessId, error_reason } = pil;
                    await report_db_connection.execute("INSERT INTO image_logs (refId, parentBusinessId, businessId, error_reason, createdAt, logCreatedAt) VALUES (?,?,?,?,?,?)",
                        [id, parentBusinessId, businessId, error_reason, createdAt, currentTimestamp]
                    )
                } catch (error) {
                    console.error(`Error inserting image_logs row (id: ${pil?.id}):`, error.message);
                }
            }

            console.timeEnd("Sync image logs cron completed in");
        } catch (err) {
            console.error(`🔥 Sync image logs cron error: ${err.message}`);
        } finally {
            isSyncImageLogsCronRunning = false;
        }
    });
};

export { syncImageLogsCron };
