import { mainPool } from "../config/db";

export const matchedOrders = async (created_at: string) => {
  try {
    const query = `
      SELECT DISTINCT
        mo.create_order_id,
        mo.source_swap_id,
        mo.destination_swap_id,
        co.created_at,
        co.source_chain,
        co.destination_chain,
        s1.updated_at AS source_updated_at,
        s1.initiate_tx_hash AS source_init_tx_hash,
        s1.redeem_tx_hash AS source_redeem_tx_hash,
        s1.refund_tx_hash AS source_refund_tx_hash,
        s1.initiate_block_number AS source_init_block_number,
        s1.redeem_block_number AS source_redeem_block_number,
        s1.refund_block_number AS source_refund_block_number,
        s2.updated_at AS destination_updated_at,
        s2.initiate_tx_hash AS destination_init_tx_hash,
        s2.redeem_tx_hash AS destination_redeem_tx_hash,
        s2.refund_tx_hash AS destination_refund_tx_hash,
        s2.initiate_block_number AS destination_init_block_number,
        s2.redeem_block_number AS destination_redeem_block_number,
        s2.refund_block_number AS destination_refund_block_number,
        co.secret_hash
      FROM create_orders co
      INNER JOIN matched_orders mo ON co.create_id = mo.create_order_id
      INNER JOIN swaps s1 ON mo.source_swap_id = s1.swap_id
      INNER JOIN swaps s2 ON mo.destination_swap_id = s2.swap_id
      WHERE (s1.redeem_tx_hash IS NOT NULL AND s1.redeem_tx_hash != '' 
          OR s1.refund_tx_hash IS NOT NULL AND s1.refund_tx_hash != '')
          AND (s2.redeem_tx_hash IS NOT NULL AND s2.redeem_tx_hash != '' 
          OR s2.refund_tx_hash IS NOT NULL AND s2.refund_tx_hash != '')
          AND (s1.redeem_block_number IS NOT NULL AND s1.redeem_block_number > 0 
          OR s1.refund_block_number IS NOT NULL AND s1.refund_block_number > 0)
          AND (s2.redeem_block_number IS NOT NULL AND s2.redeem_block_number > 0 
          OR s2.refund_block_number IS NOT NULL AND s2.refund_block_number > 0)
          AND s1.initiate_block_number IS NOT NULL AND s1.initiate_block_number > 0
          AND s2.initiate_block_number IS NOT NULL AND s2.initiate_block_number > 0
          AND mo.created_at >= $1::timestamp
    `;

    await mainPool.query("SET TIME ZONE 'UTC'");
    const result = await mainPool.query(query, [created_at]);

    return result.rows;
  } catch (err: any) {
    console.error("Error fetching matched orders:", err.message);
    throw err;
  }
};