import { prod_db_connection, report_db_connection } from "../db/mysql.js";
const syncBusinessStatusController = async (req, res) => {
  const { id, status, onlineOrdersDelivery, onlineOrdersSelfPickup, inCarOrders, dineInOrders } = req.body;

  if (id == undefined || status == undefined || onlineOrdersDelivery == undefined || onlineOrdersSelfPickup == undefined || inCarOrders == undefined || dineInOrders == undefined) {
    return res.status(400).json({ success: false, msg: "All fields are required." });
  }

  let [business] = await report_db_connection.execute("SELECT id FROM addo_business WHERE id = ?", [id]);
  if (!business?.length) return res.status(400).json({ success: false, msg: "Business not found in report db." });

  await report_db_connection.execute(`UPDATE addo_business SET status = ?, onlineOrdersDelivery = ?, onlineOrdersSelfPickup = ?, inCarOrders = ?, dineInOrders = ?, updatedAt = NOW() WHERE id = ?`, [
    Number(status),
    Number(onlineOrdersDelivery),
    Number(onlineOrdersSelfPickup),
    Number(inCarOrders),
    Number(dineInOrders),
    Number(id),
  ]);
  res.status(200).json({ success: true, msg: "Business updated successfully." });
};

const syncOrdersController = async (req, res) => {
  // /*
  // Fetch orders from production ( ONLY FOR TESTING )
  const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  console.time("QUERY");
  const [orders] = await prod_db_connection.execute(
    `
    SELECT 
        ao.id AS orderId,
        ao.invoicedDate,
        ao.invoicedAt,
        ab.id AS businessId,
        ao.total,
        ao.deliveryStatus,
        ao.orderType
      FROM addo_orders ao
      LEFT JOIN addo_business ab ON ao.businessId = ab.id
      WHERE ao.status = 1
        AND ao.invoicedDate = ?
        AND ab.parentId = 5
        `,
    [today]
  );
  console.timeEnd("QUERY");
  // */

  // const orders = req.body;

  if (!Array.isArray(orders) || !orders.length) return res.status(400).json({ success: false, msg: "Expected non-empty array of orders." });

  // Process in batches of 20
  const batchSize = 100;
  const totalBatches = Math.ceil(orders.length / batchSize);

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const startIndex = batchIndex * batchSize;
    const endIndex = Math.min(startIndex + batchSize, orders.length);
    const currentBatch = orders.slice(startIndex, endIndex);

    console.time(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);
    // Process each orders in the current batch in parallel
    const batchPromises = currentBatch.map(async (pOrder) => {
      const orderFields = ["orderId", "invoicedDate", "invoicedAt", "businessId", "total", "deliveryStatus", "orderType"];
      const orderFieldsValues = orderFields.map((of) => pOrder[of]);

      let [order] = await report_db_connection.execute("SELECT orderId FROM addo_orders WHERE orderId = ?", [pOrder.orderId]);
      if (order?.length) {
        let setFields = orderFields.map((of) => `${of} = ?`).join(",");
        await report_db_connection.execute(`UPDATE addo_orders SET ${setFields} WHERE orderId = ?`, [...orderFieldsValues, pOrder.orderId]);
      } else {
        let fields = orderFields.join(",");
        let placeholders = orderFields.map(() => "?").join(",");
        await report_db_connection.execute(`INSERT INTO addo_orders (${fields}) VALUES (${placeholders})`, [...orderFieldsValues]);
      }
    });

    await Promise.all(batchPromises);
    console.timeEnd(`Processing batch ${batchIndex + 1}/${totalBatches} with ${currentBatch.length} items`);

    // // Process each orders in the current batch
    // for (const pOrder of currentBatch) {
    //   const orderFields = ["orderId", "invoicedDate", "invoicedAt", "businessId", "total", "deliveryStatus", "orderType"];
    //   const orderFieldsValues = orderFields.map((of) => pOrder[of]);

    //   let [order] = await report_db_connection.execute("SELECT orderId FROM addo_orders WHERE orderId = ?", [pOrder.orderId]);
    //   if (order?.length) {
    //     let setFields = orderFields.map((of) => `${of} = ?`).join(",");
    //     await report_db_connection.execute(`UPDATE addo_orders SET ${setFields} WHERE orderId = ?`, [...orderFieldsValues, pOrder.orderId]);
    //   } else {
    //     let fields = orderFields.join(",");
    //     let placeholders = orderFields.map(() => "?").join(",");
    //     await report_db_connection.execute(`INSERT INTO addo_orders (${fields}) VALUES (${placeholders})`, [...orderFieldsValues]);
    //   }
    // }

    // Optional: Add a small delay between batches to prevent overwhelming the database
    if (batchIndex < totalBatches - 1) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  res.status(200).json({ success: true, msg: "Order synced successfully." });
};

export { syncBusinessStatusController, syncOrdersController };
