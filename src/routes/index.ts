import { Router } from "express";
import { getChainCombinationAverages, getAllIndividualOrders, getAnomalyOrders } from "../controllers/orderController";
import { syncOrders, updateTimestamps } from "../controllers/syncController";
import {getPaginatedMatchedOrders} from '../controllers/matchedController'
const router = Router();

router.post("/averages", getChainCombinationAverages);
router.post("/orders/all", getAllIndividualOrders);
router.post("/orders/anomalies", getAnomalyOrders);
router.post("/sync", syncOrders);
router.post("/updateTimestamps", updateTimestamps);
router.get("/matched", getPaginatedMatchedOrders);

export default router;