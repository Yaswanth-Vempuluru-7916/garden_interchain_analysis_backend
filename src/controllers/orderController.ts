import { Request, Response, NextFunction } from "express";
import { TimeframeRequestBody } from "../types";
import { supportedChains } from "../config/db";
import { analysisPool , ORDERS_TABLE} from "../config/db";
import { matchedOrders } from "../services/matchedOrderService";

const ANOMALY_THRESHOLD = 1.5 * 3600; // 5400 seconds

export const getChainCombinationAverages = async (
  req: Request<{}, {}, TimeframeRequestBody>,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { start_time, end_time } = req.body;

  const defaultEndTime = new Date().toISOString();
  const defaultStartTime = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const queryStartTime = start_time || defaultStartTime;
  const queryEndTime = end_time || defaultEndTime;

  const isValidISODate = (date: string) => !isNaN(Date.parse(date));
  if (start_time && !isValidISODate(start_time) || end_time && !isValidISODate(end_time)) {
    res.status(400).json({ error: "Invalid date format" });
    return;
  }

  const query = `
    SELECT
      source_chain,
      destination_chain,
      COUNT(*) AS total_orders,
      AVG(CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END) AS avg_user_init_duration,
      COALESCE(
        AVG(CASE 
          WHEN cobi_init IS NOT NULL 
          AND user_init IS NOT NULL 
          AND (user_redeem IS NOT NULL OR cobi_redeem IS NOT NULL) 
          AND user_refund IS NULL 
          AND cobi_refund IS NULL 
          THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) 
        END),
        0
      ) AS avg_cobi_init_duration,
      AVG(CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END) AS avg_user_redeem_duration,
      AVG(CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END) AS avg_user_refund_duration,
      AVG(CASE WHEN cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) END) AS avg_cobi_redeem_duration,
      AVG(CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END) AS avg_cobi_refund_duration
    FROM ${ORDERS_TABLE}
    WHERE created_at BETWEEN $1 AND $2
      AND (
        user_init IS NOT NULL OR
        cobi_init IS NOT NULL OR
        user_redeem IS NOT NULL OR
        user_refund IS NOT NULL OR
        cobi_redeem IS NOT NULL OR
        cobi_refund IS NOT NULL
      )
      AND source_chain = ANY($3)
      AND destination_chain = ANY($4) 
      AND (
        (user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) <= ${ANOMALY_THRESHOLD})
        AND (cobi_init IS NULL OR user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) <= ${ANOMALY_THRESHOLD})
        AND (user_redeem IS NULL OR user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) <= ${ANOMALY_THRESHOLD})
        AND (cobi_redeem IS NULL OR cobi_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) <= ${ANOMALY_THRESHOLD})
      )
    GROUP BY source_chain, destination_chain
    ORDER BY source_chain, destination_chain;
  `;

  const lastUpdatedQuery = `
    SELECT MAX(created_at) AS last_updated
    FROM ${ORDERS_TABLE}
    WHERE (
        (user_init_block_number IS NOT NULL AND user_init IS NOT NULL) OR
        (user_redeem_block_number IS NOT NULL AND user_redeem IS NOT NULL) OR
        (user_refund_block_number IS NOT NULL AND user_refund IS NOT NULL) OR
        (cobi_init_block_number IS NOT NULL AND cobi_init IS NOT NULL) OR
        (cobi_redeem_block_number IS NOT NULL AND cobi_redeem IS NOT NULL) OR
        (cobi_refund_block_number IS NOT NULL AND cobi_refund IS NOT NULL)
);
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains,
    ]);

    const lastUpdatedResult = await analysisPool.query(lastUpdatedQuery);
    const lastUpdated =
      lastUpdatedResult.rows[0]?.last_updated?.toISOString() || "1970-01-01T00:00:00.000Z";

    const averagesByChain = result.rows.reduce((acc: any, row: any) => {
      const key = `${row.source_chain}-${row.destination_chain}`;
      acc[key] = {
        total_orders: parseInt(row.total_orders, 10),
        avg_user_init_duration: row.avg_user_init_duration
          ? parseFloat(row.avg_user_init_duration)
          : null,
        avg_cobi_init_duration: parseFloat(row.avg_cobi_init_duration),
        avg_user_redeem_duration: row.avg_user_redeem_duration
          ? parseFloat(row.avg_user_redeem_duration)
          : null,
        avg_user_refund_duration: row.avg_user_refund_duration
          ? parseFloat(row.avg_user_refund_duration)
          : null,
        avg_cobi_redeem_duration: row.avg_cobi_redeem_duration
          ? parseFloat(row.avg_cobi_redeem_duration)
          : null,
        avg_cobi_refund_duration: row.avg_cobi_refund_duration
          ? parseFloat(row.avg_cobi_refund_duration)
          : null,
      };
      return acc;
    }, {});

    const chainCombinations: any = {};
    supportedChains.forEach((source) => {
      supportedChains.forEach((destination) => {
        const key = `${source}-${destination}`;
        chainCombinations[key] = averagesByChain[key] || {
          total_orders: 0,
          avg_user_init_duration: null,
          avg_cobi_init_duration: null,
          avg_user_redeem_duration: null,
          avg_user_refund_duration: null,
          avg_cobi_redeem_duration: null,
          avg_cobi_refund_duration: null,
        };
      });
    });

    res.json({
      message: "Average durations for all chain combinations (in seconds, excluding anomalies)",
      last_updated: lastUpdated,
      averages: chainCombinations,
    });
  } catch (err: any) {
    console.error("Error calculating chain combination averages:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
};

export const getAllIndividualOrders = async (
  req: Request<{}, {}, TimeframeRequestBody>,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { start_time, end_time } = req.body;

  const defaultEndTime = new Date().toISOString();
  const defaultStartTime = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const queryStartTime = start_time || defaultStartTime;
  const queryEndTime = end_time || defaultEndTime;

  const isValidISODate = (date: string) => !isNaN(Date.parse(date));
  if (start_time && !isValidISODate(start_time) || end_time && !isValidISODate(end_time)) {
    res.status(400).json({ error: "Invalid date format" });
    return;
  }

  const query = `
    SELECT
      source_chain,
      destination_chain,
      create_order_id,
      created_at,
      CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END AS user_init_duration,
      CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END AS cobi_init_duration,
      CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END AS user_redeem_duration,
      CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END AS user_refund_duration,
      CASE WHEN cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) END AS cobi_redeem_duration,
      CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END AS cobi_refund_duration,
      (
        COALESCE(CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END, 0) +
        COALESCE(CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END, 0) +
        COALESCE(CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END, 0)
      ) AS overall_duration
    FROM ${ORDERS_TABLE}
    WHERE created_at BETWEEN $1 AND $2
      AND (
        user_init IS NOT NULL OR
        cobi_init IS NOT NULL OR
        user_redeem IS NOT NULL OR
        user_refund IS NOT NULL OR
        cobi_redeem IS NOT NULL OR
        cobi_refund IS NOT NULL
      )
      AND source_chain = ANY($3)
      AND destination_chain = ANY($4)
      AND (
        (user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) <= ${ANOMALY_THRESHOLD})
        AND (cobi_init IS NULL OR user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) <= ${ANOMALY_THRESHOLD})
        AND (user_redeem IS NULL OR user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) <= ${ANOMALY_THRESHOLD})
        AND (cobi_redeem IS NULL OR cobi_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) <= ${ANOMALY_THRESHOLD})
      )
    ORDER BY source_chain, destination_chain, created_at ASC;
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains,
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: "No orders found with timestamps in the given range (excluding anomalies)" });
      return;
    }

    const ordersByChain = result.rows.reduce((acc: any, order: any) => {
      const key = `${order.source_chain}-${order.destination_chain}`;
      if (!acc[key]) {
        acc[key] = [];
      }
      acc[key].push({
        create_order_id: order.create_order_id,
        created_at: order.created_at.toISOString(),
        durations: {
          user_init_duration: order.user_init_duration ? parseFloat(order.user_init_duration) : null,
          cobi_init_duration: order.cobi_init_duration ? parseFloat(order.cobi_init_duration) : null,
          user_redeem_duration: order.user_redeem_duration ? parseFloat(order.user_redeem_duration) : null,
          user_refund_duration: order.user_refund_duration ? parseFloat(order.user_refund_duration) : null,
          cobi_redeem_duration: order.cobi_redeem_duration ? parseFloat(order.cobi_redeem_duration) : null,
          cobi_refund_duration: order.cobi_refund_duration ? parseFloat(order.cobi_refund_duration) : null,
          overall_duration: order.overall_duration ? parseFloat(order.overall_duration) : null,
        },
      });
      return acc;
    }, {});

    res.json({
      message: "Individual order durations for all chain combinations (in seconds, excluding anomalies)",
      orders: ordersByChain,
    });
  } catch (err: any) {
    console.error("Error fetching individual orders:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
};

export const getAnomalyOrders = async (
  req: Request<{}, {}, TimeframeRequestBody>,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { start_time, end_time } = req.body;

  const defaultEndTime = new Date().toISOString();
  const defaultStartTime = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const queryStartTime = start_time || defaultStartTime;
  const queryEndTime = end_time || defaultEndTime;

  const isValidISODate = (date: string) => !isNaN(Date.parse(date));
  if (start_time && !isValidISODate(start_time) || end_time && !isValidISODate(end_time)) {
    res.status(400).json({ error: "Invalid date format" });
    return;
  }

  const query = `
    SELECT
      source_chain,
      destination_chain,
      create_order_id,
      created_at,
      CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END AS user_init_duration,
      CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END AS cobi_init_duration,
      CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END AS user_redeem_duration,
      CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END AS user_refund_duration,
      CASE WHEN cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) END AS cobi_redeem_duration,
      CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END AS cobi_refund_duration,
      (
        COALESCE(CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END, 0) +
        COALESCE(CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END, 0) +
        COALESCE(CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) END, 0) +
        COALESCE(CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END, 0)
      ) AS overall_duration
    FROM ${ORDERS_TABLE}
    WHERE created_at BETWEEN $1 AND $2
      AND (
        user_init IS NOT NULL OR
        cobi_init IS NOT NULL OR
        user_redeem IS NOT NULL OR
        user_refund IS NOT NULL OR
        cobi_redeem IS NOT NULL OR
        cobi_refund IS NOT NULL
      )
      AND source_chain = ANY($3)
      AND destination_chain = ANY($4)
      AND (
        (user_init IS NOT NULL AND GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) > ${ANOMALY_THRESHOLD})
        OR (cobi_init IS NOT NULL AND user_init IS NOT NULL AND GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) > ${ANOMALY_THRESHOLD})
        OR (user_redeem IS NOT NULL AND user_init IS NOT NULL AND GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) > ${ANOMALY_THRESHOLD})
        OR (cobi_redeem IS NOT NULL AND cobi_init IS NOT NULL AND GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - cobi_init)), 0) > ${ANOMALY_THRESHOLD})
      )
    ORDER BY source_chain, destination_chain, created_at ASC;
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains,
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: "No anomalous orders found in the given range" });
      return;
    }

    const ordersByChain = result.rows.reduce((acc: any, order: any) => {
      const key = `${order.source_chain}-${order.destination_chain}`;
      if (!acc[key]) {
        acc[key] = [];
      }
      acc[key].push({
        create_order_id: order.create_order_id,
        created_at: order.created_at.toISOString(),
        durations: {
          user_init_duration: order.user_init_duration ? parseFloat(order.user_init_duration) : null,
          cobi_init_duration: order.cobi_init_duration ? parseFloat(order.cobi_init_duration) : null,
          user_redeem_duration: order.user_redeem_duration ? parseFloat(order.user_redeem_duration) : null,
          user_refund_duration: order.user_refund_duration ? parseFloat(order.user_refund_duration) : null,
          cobi_redeem_duration: order.cobi_redeem_duration ? parseFloat(order.cobi_redeem_duration) : null,
          cobi_refund_duration: order.cobi_refund_duration ? parseFloat(order.cobi_refund_duration) : null,
          overall_duration: order.overall_duration ? parseFloat(order.overall_duration) : null,
        },
      });
      return acc;
    }, {});

    res.json({
      message: "Anomalous order durations for all chain combinations (in seconds)",
      orders: ordersByChain,
    });
  } catch (err: any) {
    console.error("Error fetching anomalous orders:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
};

export const getMatchedOrders = async (
  req: Request<{}, {}, { created_at: string }>,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const { created_at } = req.body;

    if (!created_at || isNaN(Date.parse(created_at))) {
      res.status(400).json({ error: "Valid created_at date is required" });
      return;
    }

    const orders = await matchedOrders(created_at);

    if (!orders || orders.length === 0) {
      res.status(404).json({ message: "No matched orders found for the given date" });
      return;
    }

    res.status(200).json(orders);
  } catch (err: any) {
    console.error("Error fetching matched orders:", err.message);
    res.status(500).json({ error: "Internal server error" });
  }
};