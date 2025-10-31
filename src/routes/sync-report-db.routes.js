import { Router } from "express";
import { syncBusinessStatusController, syncOrdersController } from "../controllers/sync.controllers.js";

const router = Router();

router.post("/business-status", syncBusinessStatusController);
router.post("/orders", syncOrdersController);

export { router as syncReportDBRoutes };
