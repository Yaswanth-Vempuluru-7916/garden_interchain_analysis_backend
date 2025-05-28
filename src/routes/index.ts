import { Router } from "express";
import { getChainCombinationAverages, getAllIndividualOrders, getAnomalyOrders } from "../controllers/orderController";
import { syncOrders, updateTimestamps } from "../controllers/syncController";
import {getAllSuccessfulMatchedOrders, getPaginatedMatchedOrders} from '../controllers/matchedController'
const router = Router();

router.post("/averages", getChainCombinationAverages);
router.post("/orders/all", getAllIndividualOrders);
router.post("/orders/anomalies", getAnomalyOrders);
router.post("/sync", syncOrders);
router.post("/updateTimestamps", updateTimestamps);
router.get("/matched", getPaginatedMatchedOrders);
router.get("/matched/successful", getAllSuccessfulMatchedOrders);

export default router;