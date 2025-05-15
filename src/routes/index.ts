import { Router } from "express";
import { getChainCombinationAverages, getAllIndividualOrders, getAnomalyOrders } from "../controllers/orderController";
import { getMatchedOrders } from "../controllers/orderController";
import { syncOrders, updateTimestamps } from "../controllers/syncController";

const router = Router();

router.post("/averages", getChainCombinationAverages);
router.post("/orders/all", getAllIndividualOrders);
router.post("/orders/anomalies", getAnomalyOrders);
router.post("/sync", syncOrders);
router.post("/updateTimestamps", updateTimestamps);
router.post("/orders/matched", getMatchedOrders); 

export default router;