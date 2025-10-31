import express from "express";

import { syncReportDBRoutes } from "./routes/sync-report-db.routes.js";

const app = express();

app.use(express.json());

app.use("/sync", syncReportDBRoutes);

app.use((err, req, res, next) => {
  if (err) return res.status(500).json({ success: true, msg: `Internal Server Error: ${err.message}` });
  next();
});

export { app };
