import { Request, Response } from "express";
import { analysisPool, ORDERS_TABLE } from "../config/db";
import { initTable, populateOrderAnalysis } from "../services/dbService";
import { updateTimestampsForOrders } from "../services/blockTimestampUpdate";

export const syncOrders = async (req: Request, res: Response): Promise<void> => {
  try {
    await initTable();
    await populateOrderAnalysis();
    res.status(200).json({ message: "Order sync completed successfully" });
  } catch (err: any) {
    console.error("Error during order sync:", err.message);
    res.status(500).json({ error: "Order sync failed" });
  }
};

export const updateTimestamps = async (req: Request, res: Response): Promise<void> => {
  try {
    const orderIdsQuery = `
      SELECT create_order_id
      FROM ${ORDERS_TABLE}
      WHERE (
        (user_init_block_number IS NOT NULL AND user_init IS NULL) OR
        (user_redeem_block_number IS NOT NULL AND user_redeem IS NULL) OR
        (user_refund_block_number IS NOT NULL AND user_refund IS NULL) OR
        (cobi_init_block_number IS NOT NULL AND cobi_init IS NULL) OR
        (cobi_redeem_block_number IS NOT NULL AND cobi_redeem IS NULL) OR
        (cobi_refund_block_number IS NOT NULL AND cobi_refund IS NULL)
      )
    `;
    const orderIdsResult = await analysisPool.query(orderIdsQuery);
    const orderIds = orderIdsResult.rows.map((row) => row.create_order_id);
    await updateTimestampsForOrders(orderIds);
    res.status(200).json({ message: "Timestamps updated successfully" });
  } catch (err: any) {
    console.error("Error in /updateTimestamps:", err.message);
    res.status(500).json({ error: "Failed to update timestamps" });
  }
};

