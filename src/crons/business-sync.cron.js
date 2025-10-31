import moment from "moment";
import { schedule } from "node-cron";
import { prod_db_connection, report_db_connection } from "../db/mysql.js";
import { CRON_EXPRESSIONS } from "../configs/env.js";

const syncBusinessCron = () => {
  console.log("Sync Business cron enabled.");

  let isSyncBusinessCronRunning = false;
  let isSyncBusinessUpdatesCronRunning = false;
  let isSyncBusinessReviewUpdateCronRunning = false;

  // Sync overall business - once a day
  schedule(CRON_EXPRESSIONS.syncBusinessCronExp, async () => {
    if (isSyncBusinessCronRunning) return console.log("Previous sync business cron still running. Skipping this cycle.");
    isSyncBusinessCronRunning = true;

    console.time("Sync business cron completed in");
    console.log("Sync business cron started");
    try {
      const businessFields = [
        "id",
        "parentId",
        "isParent",
        "slug",
        "name",
        "legalEntityName",
        "gstin",
        "city",
        "state",
        "locality",
        "address",
        "postal_code",
        "latitude",
        "longitude",
        "contactNumber",
        "menu_score",
        "status",
        "is_opened",
        "onlineOrders",
        "onlineOrdersDelivery",
        "onlineOrdersSelfPickup",
        "inCarOrders",
        "dineInOrders",
        "currency",
        "ondc",
        "source",
        "launch_date",
      ];

      // Set all report business status to -1
      await report_db_connection.execute(`UPDATE addo_business SET status = -1`);

      // Get all businesses that match the criteria
      const [prodBusinesses] = await prod_db_connection.execute(`SELECT ${businessFields.join(",")} FROM addo_business WHERE status = 1 AND (parentId = 5 OR id = 5) AND id NOT IN (89, 8096, 8097)`);

      // Process in batches of 20
      const batchSize = 20;
      const totalBatches = Math.ceil(prodBusinesses.length / batchSize);

      for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const startIndex = batchIndex * batchSize;
        const endIndex = Math.min(startIndex + batchSize, prodBusinesses.length);
        const currentBatch = prodBusinesses.slice(startIndex, endIndex);

        console.time(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Process each business in the current batch in parallel
        const batchPromises = currentBatch.map(async (pb) => {
          let currBValues = businessFields.map((bf) => pb[bf]);

          let [business] = await report_db_connection.execute("SELECT id FROM addo_business WHERE id = ?", [pb.id]);

          if (business?.length) {
            let setFields = businessFields.map((bf) => `${bf} = ?`).join(",");
            await report_db_connection.execute(`UPDATE addo_business SET ${setFields}, updatedAt = NOW() WHERE id = ?`, [...currBValues, pb.id]);
          } else {
            let fields = businessFields.join(",");
            let placeholders = businessFields.map(() => "?").join(",");
            await report_db_connection.execute(`INSERT INTO addo_business (${fields}) VALUES (${placeholders})`, currBValues);
          }
        });
        await Promise.all(batchPromises);

        console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Optional: Add a small delay between batches to prevent overwhelming the database
        if (batchIndex < totalBatches - 1) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      console.timeEnd("Sync business cron completed in");
    } catch (error) {
      console.log(`Sync business cron error: ${error.message}`);
    } finally {
      isSyncBusinessCronRunning = false;
    }
  });

  // Sync business menu score - once a day
  schedule(CRON_EXPRESSIONS.syncBusinessMenuScoreCronExp, async () => {
    if (isSyncBusinessCronRunning) return console.log("Previous sync business menu score cron still running. Skipping this cycle.");
    isSyncBusinessCronRunning = true;

    console.time("Sync business menu score cron completed in");
    console.log("Sync business menu score cron started");
    try {

      // Get all businesses that match the criteria
      const [prodBusinesses] = await prod_db_connection.execute(`SELECT id, menu_score FROM addo_business WHERE status = 1 AND (parentId = 5 OR id = 5) AND id NOT IN (89, 8096, 8097)`);

      // Process in batches of 20
      const batchSize = 20;
      const totalBatches = Math.ceil(prodBusinesses.length / batchSize);

      for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const startIndex = batchIndex * batchSize;
        const endIndex = Math.min(startIndex + batchSize, prodBusinesses.length);
        const currentBatch = prodBusinesses.slice(startIndex, endIndex);

        console.time(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Process each business in the current batch in parallel
        const batchPromises = currentBatch.map(async (pb) => {
          let [business] = await report_db_connection.execute("SELECT id FROM addo_business WHERE id = ?", [pb.id]);

          let currentTime = moment().format("YYYY-MM-DD HH:mm:ss");
          if (business?.length) {
            await report_db_connection.execute(`UPDATE addo_business SET menu_score = ?, updatedAt = ? WHERE id = ?`, [pb?.menu_score, currentTime, pb.id]);
          } else {
            console.log(`Outlet not found in report db: id: ${pb.id}`);
          }
        });
        await Promise.all(batchPromises);

        console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Optional: Add a small delay between batches to prevent overwhelming the database
        if (batchIndex < totalBatches - 1) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      console.timeEnd("Sync business menu score cron completed in");
    } catch (error) {
      console.log(`Sync business menu score cron error: ${error.message}`);
    } finally {
      isSyncBusinessCronRunning = false;
    }
  });

  // Sync business orderType changes near real time - every 10 minutes
  schedule(CRON_EXPRESSIONS.syncBusinessOrderTypeCronExp, async () => {
    if (isSyncBusinessUpdatesCronRunning) return console.log("Previous sync business update cron still running. Skipping this cycle.");
    isSyncBusinessUpdatesCronRunning = true;

    console.time("Sync business updates cron completed in");
    console.log("Sync business updates cron started");
    try {
      // Get business changes
      const [mabus] = await prod_db_connection.execute("SELECT * from metabase_orderType_update WHERE status = 0 ORDER BY id ASC LIMIT 1000");
      if (mabus?.length) {
        let mabusIds = mabus.map((mabu) => mabu.id);
        let placeholders = mabusIds.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_orderType_update SET status = 2 WHERE id IN (${placeholders})`, mabusIds);
      } else {
        isSyncBusinessUpdatesCronRunning = false;
        console.log("No business updates to sync");
        console.timeEnd("Sync business updates cron completed in");
        return;
      }

      let success_mabus = [];
      let failed_mabus = [];

      for (const mabu of mabus) {
        const { id, businessId, orderType, orderTypeStatus } = mabu;
        const orderTypeName = { 1: "onlineOrdersDelivery", 2: "onlineOrdersSelfPickup", 3: "dineInOrders", 4: "inCarOrders" };
        try {
          const [rab] = await report_db_connection.execute("SELECT * FROM addo_business WHERE id = ?", [businessId]);
          if (rab?.length) {
            await report_db_connection.execute(`UPDATE addo_business SET ${orderTypeName[orderType]}=? WHERE id = ?`, [orderTypeStatus, businessId]);
            success_mabus.push(id);
          } else {
            failed_mabus.push(id);
          }
        } catch (error) {
          failed_mabus.push(id);
        }
      }

      if (success_mabus.length) {
        let placeholders = success_mabus.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_orderType_update SET status = 1 WHERE id IN (${placeholders})`, success_mabus); // Marked as success
      }

      if (failed_mabus.length) {
        let placeholders = failed_mabus.map(() => "?").join(",");
        await prod_db_connection.execute(`UPDATE metabase_orderType_update SET status = -1 WHERE id IN (${placeholders})`, failed_mabus); // Marked as failed
      }

      console.timeEnd("Sync business updates cron completed in");
    } catch (error) {
      console.log(`Sync business updates cron error: ${error?.message}`);
    } finally {
      isSyncBusinessUpdatesCronRunning = false;
    }
  });

  // Sync business reviews
  schedule(CRON_EXPRESSIONS.syncBusinessReviewsCronExp, async () => {
    if (isSyncBusinessReviewUpdateCronRunning) return console.log("Previous sync business reviews cron still running. Skipping this cycle.");
    isSyncBusinessReviewUpdateCronRunning = true;

    console.time("Sync business reviews cron completed in");
    console.log("Sync business reviews cron started");
    try {

      console.time("query")
      let [prod_abr] = await prod_db_connection.execute(`select abr.businessId as  businessId, abr.avg_rating from addo_business_reviews abr 
        left join addo_business ab ON ab.id = abr.businessId
        where ab.status = 1 AND ab.parentId = 5 AND ab.id NOT IN (89, 8096, 8097);
        `);
      console.timeEnd("query")

      // Process in batches of 20
      const batchSize = 20;
      const totalBatches = Math.ceil(prod_abr.length / batchSize);

      for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        const startIndex = batchIndex * batchSize;
        const endIndex = Math.min(startIndex + batchSize, prod_abr.length);
        const currentBatch = prod_abr.slice(startIndex, endIndex);

        console.time(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Process each business in the current batch in parallel
        const batchPromises = currentBatch.map(async (abri) => {
          const { businessId, avg_rating } = abri;
          let [review] = await report_db_connection.execute("SELECT id FROM addo_business_reviews WHERE businessId = ?", [abri.businessId]);

          let currentTime = moment().format("YYYY-MM-DD HH:mm:ss");
          if (review?.length) {
            await report_db_connection.execute(`UPDATE addo_business_reviews SET avgRating = ?, updatedAt = ? WHERE businessId = ?`, [avg_rating, currentTime, businessId]);
          } else {
            await report_db_connection.execute(`INSERT INTO addo_business_reviews (businessId, avgRating, insertedAt, updatedAt) VALUES (?,?,?,?)`, [businessId, avg_rating, currentTime, currentTime]);
          }
        });
        await Promise.all(batchPromises);

        console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

        // Optional: Add a small delay between batches to prevent overwhelming the database
        if (batchIndex < totalBatches - 1) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      console.timeEnd("Sync business reviews cron completed in");
    } catch (error) {
      console.log(`Sync business reviews cron error: ${error?.message}`);
    } finally {
      isSyncBusinessReviewUpdateCronRunning = false;
    }
  });
};

export { syncBusinessCron };
