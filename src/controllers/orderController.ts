import { Request, Response, NextFunction } from "express";
import { TimeframeRequestBody } from "../types";
import { supportedChains } from "../config/db";
import { analysisPool, ORDERS_TABLE } from "../config/db";

export const getChainCombinationAverages = async (
  req: Request<{}, {}, TimeframeRequestBody>,
  res: Response,
  next: NextFunction
): Promise<void> => {
  const { start_time, end_time } = req.body;
  console.log(`start time : ${start_time}, end time : ${end_time}`);

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
    -- Materialized CTE for durations
    WITH durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END AS user_init_duration,
        CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END AS cobi_init_duration,
        CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END AS user_redeem_duration,
        CASE WHEN cobi_redeem IS NOT NULL AND user_redeem IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - user_redeem)), 0) END AS cobi_redeem_duration
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
    ),
    counts AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        COUNT(user_init_duration) AS user_init_count,
        COUNT(cobi_init_duration) AS cobi_init_count,
        COUNT(user_redeem_duration) AS user_redeem_count,
        COUNT(cobi_redeem_duration) AS cobi_redeem_count
      FROM durations
      GROUP BY source_chain, destination_chain
    ),
    ordered_durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        user_init_duration,
        cobi_init_duration,
        user_redeem_duration,
        cobi_redeem_duration,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_init_duration) AS user_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_init_duration) AS cobi_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_redeem_duration) AS user_redeem_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_redeem_duration) AS cobi_redeem_rn
      FROM durations
      WHERE user_init_duration IS NOT NULL
        OR cobi_init_duration IS NOT NULL
        OR user_redeem_duration IS NOT NULL
        OR cobi_redeem_duration IS NOT NULL
    ),
    stats AS MATERIALIZED (
      SELECT
        c.source_chain,
        c.destination_chain,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.25 * (c.user_init_count + 1))),
               ((CEIL(0.25 * (c.user_init_count + 1)) - (0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.25 * (c.user_init_count + 1)), 1)) +
                ((0.25 * (c.user_init_count + 1)) - FLOOR(0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.25 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_init_duration,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.75 * (c.user_init_count + 1))),
               ((CEIL(0.75 * (c.user_init_count + 1)) - (0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.75 * (c.user_init_count + 1)), 1)) +
                ((0.75 * (c.user_init_count + 1)) - FLOOR(0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.75 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.25 * (c.cobi_init_count + 1))),
               ((CEIL(0.25 * (c.cobi_init_count + 1)) - (0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.25 * (c.cobi_init_count + 1)), 1)) +
                ((0.25 * (c.cobi_init_count + 1)) - FLOOR(0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.25 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.75 * (c.cobi_init_count + 1))),
               ((CEIL(0.75 * (c.cobi_init_count + 1)) - (0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.75 * (c.cobi_init_count + 1)), 1)) +
                ((0.75 * (c.cobi_init_count + 1)) - FLOOR(0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.75 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_init_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.25 * (c.user_redeem_count + 1))),
               ((CEIL(0.25 * (c.user_redeem_count + 1)) - (0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.25 * (c.user_redeem_count + 1)), 1)) +
                ((0.25 * (c.user_redeem_count + 1)) - FLOOR(0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.25 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_redeem_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.75 * (c.user_redeem_count + 1))),
               ((CEIL(0.75 * (c.user_redeem_count + 1)) - (0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.75 * (c.user_redeem_count + 1)), 1)) +
                ((0.75 * (c.user_redeem_count + 1)) - FLOOR(0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.75 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.25 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.25 * (c.cobi_redeem_count + 1)) - (0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.25 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.25 * (c.cobi_redeem_count + 1)) - FLOOR(0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.25 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.75 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.75 * (c.cobi_redeem_count + 1)) - (0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.75 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.75 * (c.cobi_redeem_count + 1)) - FLOOR(0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.75 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_redeem_duration
      FROM counts c
    )
    SELECT
      o.source_chain,
      o.destination_chain,
      COUNT(*) AS total_orders,
      AVG(CASE WHEN o.user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (o.user_init - o.created_at)), 0) END) AS avg_user_init_duration,
      COALESCE(
        AVG(CASE 
          WHEN o.cobi_init IS NOT NULL 
          AND o.user_init IS NOT NULL 
          AND (o.user_redeem IS NOT NULL OR o.cobi_redeem IS NOT NULL) 
          AND o.user_refund IS NULL 
          AND o.cobi_refund IS NULL 
          THEN GREATEST(EXTRACT(EPOCH FROM (o.cobi_init - o.user_init)), 0) 
        END),
        0
      ) AS avg_cobi_init_duration,
      AVG(CASE WHEN o.user_redeem IS NOT NULL AND o.user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (o.user_redeem - o.cobi_init)), 0) END) AS avg_user_redeem_duration,
      AVG(CASE WHEN o.user_refund IS NOT NULL AND o.user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (o.user_refund - o.user_init)), 0) END) AS avg_user_refund_duration,
      AVG(CASE WHEN o.cobi_redeem IS NOT NULL AND o.cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (o.cobi_redeem - o.cobi_init)), 0) END) AS avg_cobi_redeem_duration,
      AVG(CASE WHEN o.cobi_refund IS NOT NULL AND o.cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (o.cobi_refund - o.cobi_init)), 0) END) AS avg_cobi_refund_duration,
      s.q1_user_init_duration,
      s.q3_user_init_duration,
      s.q1_cobi_init_duration,
      s.q3_cobi_init_duration,
      s.q1_user_redeem_duration,
      s.q3_user_redeem_duration,
      s.q1_cobi_redeem_duration,
      s.q3_cobi_redeem_duration
    FROM ${ORDERS_TABLE} o
    JOIN stats s ON o.source_chain = s.source_chain AND o.destination_chain = s.destination_chain
    WHERE o.created_at BETWEEN $1 AND $2
      AND (
        o.user_init IS NOT NULL OR
        o.cobi_init IS NOT NULL OR
        o.user_redeem IS NOT NULL OR
        o.user_refund IS NOT NULL OR
        o.cobi_redeem IS NOT NULL OR
        o.cobi_refund IS NOT NULL
      )
      AND o.source_chain = ANY($3)
      AND o.destination_chain = ANY($4)
      AND (
        (o.user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (o.user_init - o.created_at)), 0) <= 
          (s.q3_user_init_duration + 1.5 * (s.q3_user_init_duration - s.q1_user_init_duration)))
        AND (o.cobi_init IS NULL OR o.user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (o.cobi_init - o.user_init)), 0) <= 
          (s.q3_cobi_init_duration + 1.5 * (s.q3_cobi_init_duration - s.q1_cobi_init_duration)))
        AND (o.user_redeem IS NULL OR o.user_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (o.user_redeem - o.cobi_init)), 0) <= 
          (s.q3_user_redeem_duration + 1.5 * (s.q3_user_redeem_duration - s.q1_user_redeem_duration)))
        AND (o.cobi_redeem IS NULL OR o.cobi_init IS NULL OR GREATEST(EXTRACT(EPOCH FROM (o.cobi_redeem - o.cobi_init)), 0) <= 
          (s.q3_cobi_redeem_duration + 1.5 * (s.q3_cobi_redeem_duration - s.q1_cobi_redeem_duration)))
      )
    GROUP BY o.source_chain, o.destination_chain, 
             s.q1_user_init_duration, s.q3_user_init_duration,
             s.q1_cobi_init_duration, s.q3_cobi_init_duration,
             s.q1_user_redeem_duration, s.q3_user_redeem_duration,
             s.q1_cobi_redeem_duration, s.q3_cobi_redeem_duration
    ORDER BY o.source_chain, o.destination_chain;
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

    const thresholdsByChain: any = {};
    result.rows.forEach((row: any) => {
      const key = `${row.source_chain}-${row.destination_chain}`;
      thresholdsByChain[key] = {
        user_init_duration: {
          lower: row.q1_user_init_duration && row.q3_user_init_duration
            ? parseFloat(row.q1_user_init_duration) - 1.5 * (parseFloat(row.q3_user_init_duration) - parseFloat(row.q1_user_init_duration))
            : null,
          upper: row.q1_user_init_duration && row.q3_user_init_duration
            ? parseFloat(row.q3_user_init_duration) + 1.5 * (parseFloat(row.q3_user_init_duration) - parseFloat(row.q1_user_init_duration))
            : null,
        },
        cobi_init_duration: {
          lower: row.q1_cobi_init_duration && row.q3_cobi_init_duration
            ? parseFloat(row.q1_cobi_init_duration) - 1.5 * (parseFloat(row.q3_cobi_init_duration) - parseFloat(row.q1_cobi_init_duration))
            : null,
          upper: row.q1_cobi_init_duration && row.q3_cobi_init_duration
            ? parseFloat(row.q3_cobi_init_duration) + 1.5 * (parseFloat(row.q3_cobi_init_duration) - parseFloat(row.q1_cobi_init_duration))
            : null,
        },
        user_redeem_duration: {
          lower: row.q1_user_redeem_duration && row.q3_user_redeem_duration
            ? parseFloat(row.q1_user_redeem_duration) - 1.5 * (parseFloat(row.q3_user_redeem_duration) - parseFloat(row.q1_user_redeem_duration))
            : null,
          upper: row.q1_user_redeem_duration && row.q3_user_redeem_duration
            ? parseFloat(row.q3_user_redeem_duration) + 1.5 * (parseFloat(row.q3_user_redeem_duration) - parseFloat(row.q1_user_redeem_duration))
            : null,
        },
        cobi_redeem_duration: {
          lower: row.q1_cobi_redeem_duration && row.q3_cobi_redeem_duration
            ? parseFloat(row.q1_cobi_redeem_duration) - 1.5 * (parseFloat(row.q3_cobi_redeem_duration) - parseFloat(row.q1_cobi_redeem_duration))
            : null,
          upper: row.q1_cobi_redeem_duration && row.q3_cobi_redeem_duration
            ? parseFloat(row.q3_cobi_redeem_duration) + 1.5 * (parseFloat(row.q3_cobi_redeem_duration) - parseFloat(row.q1_cobi_redeem_duration))
            : null,
        },
      };
    });

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
      message: "Average durations for all chain combinations (in seconds, excluding anomalies via IQR)",
      last_updated: lastUpdated,
      averages: chainCombinations,
      thresholds: thresholdsByChain,
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
    WITH durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        create_order_id,
        created_at,
        CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END AS user_init_duration,
        CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END AS cobi_init_duration,
        CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END AS user_redeem_duration,
        CASE WHEN cobi_redeem IS NOT NULL AND user_redeem IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - user_redeem)), 0) END AS cobi_redeem_duration,
        CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END AS user_refund_duration,
        CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END AS cobi_refund_duration
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
    ),
    counts AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        COUNT(user_init_duration) AS user_init_count,
        COUNT(cobi_init_duration) AS cobi_init_count,
        COUNT(user_redeem_duration) AS user_redeem_count,
        COUNT(cobi_redeem_duration) AS cobi_redeem_count
      FROM durations
      GROUP BY source_chain, destination_chain
    ),
    ordered_durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        user_init_duration,
        cobi_init_duration,
        user_redeem_duration,
        cobi_redeem_duration,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_init_duration) AS user_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_init_duration) AS cobi_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_redeem_duration) AS user_redeem_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_redeem_duration) AS cobi_redeem_rn
      FROM durations
      WHERE user_init_duration IS NOT NULL
        OR cobi_init_duration IS NOT NULL
        OR user_redeem_duration IS NOT NULL
        OR cobi_redeem_duration IS NOT NULL
    ),
    stats AS MATERIALIZED (
      SELECT
        c.source_chain,
        c.destination_chain,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.25 * (c.user_init_count + 1))),
               ((CEIL(0.25 * (c.user_init_count + 1)) - (0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.25 * (c.user_init_count + 1)), 1)) +
                ((0.25 * (c.user_init_count + 1)) - FLOOR(0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.25 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_init_duration,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.75 * (c.user_init_count + 1))),
               ((CEIL(0.75 * (c.user_init_count + 1)) - (0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.75 * (c.user_init_count + 1)), 1)) +
                ((0.75 * (c.user_init_count + 1)) - FLOOR(0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.75 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.25 * (c.cobi_init_count + 1))),
               ((CEIL(0.25 * (c.cobi_init_count + 1)) - (0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.25 * (c.cobi_init_count + 1)), 1)) +
                ((0.25 * (c.cobi_init_count + 1)) - FLOOR(0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.25 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.75 * (c.cobi_init_count + 1))),
               ((CEIL(0.75 * (c.cobi_init_count + 1)) - (0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.75 * (c.cobi_init_count + 1)), 1)) +
                ((0.75 * (c.cobi_init_count + 1)) - FLOOR(0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.75 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_init_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.25 * (c.user_redeem_count + 1))),
               ((CEIL(0.25 * (c.user_redeem_count + 1)) - (0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.25 * (c.user_redeem_count + 1)), 1)) +
                ((0.25 * (c.user_redeem_count + 1)) - FLOOR(0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.25 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_redeem_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.75 * (c.user_redeem_count + 1))),
               ((CEIL(0.75 * (c.user_redeem_count + 1)) - (0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.75 * (c.user_redeem_count + 1)), 1)) +
                ((0.75 * (c.user_redeem_count + 1)) - FLOOR(0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.75 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.25 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.25 * (c.cobi_redeem_count + 1)) - (0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.25 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.25 * (c.cobi_redeem_count + 1)) - FLOOR(0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.25 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.75 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.75 * (c.cobi_redeem_count + 1)) - (0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.75 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.75 * (c.cobi_redeem_count + 1)) - FLOOR(0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.75 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_redeem_duration
      FROM counts c
    )
    SELECT
      d.source_chain,
      d.destination_chain,
      d.create_order_id,
      d.created_at,
      d.user_init_duration,
      d.cobi_init_duration,
      d.user_redeem_duration,
      d.user_refund_duration,
      d.cobi_redeem_duration,
      d.cobi_refund_duration,
      (
        COALESCE(d.user_init_duration, 0) +
        COALESCE(d.cobi_init_duration, 0) +
        COALESCE(d.user_redeem_duration, 0) +
        COALESCE(d.user_refund_duration, 0) +
        COALESCE(d.cobi_redeem_duration, 0) +
        COALESCE(d.cobi_refund_duration, 0)
      ) AS overall_duration
    FROM durations d
    JOIN stats s ON d.source_chain = s.source_chain AND d.destination_chain = s.destination_chain
    WHERE (
        (d.user_init_duration IS NULL OR d.user_init_duration <= 
          (s.q3_user_init_duration + 1.5 * (s.q3_user_init_duration - s.q1_user_init_duration)))
        AND (d.cobi_init_duration IS NULL OR d.cobi_init_duration <= 
          (s.q3_cobi_init_duration + 1.5 * (s.q3_cobi_init_duration - s.q1_cobi_init_duration)))
        AND (d.user_redeem_duration IS NULL OR d.user_redeem_duration <= 
          (s.q3_user_redeem_duration + 1.5 * (s.q3_user_redeem_duration - s.q1_user_redeem_duration)))
        AND (d.cobi_redeem_duration IS NULL OR d.cobi_redeem_duration <= 
          (s.q3_cobi_redeem_duration + 1.5 * (s.q3_cobi_redeem_duration - s.q1_cobi_redeem_duration)))
      )
    ORDER BY d.source_chain, d.destination_chain, d.created_at ASC;
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains,
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: "No non-anomalous orders found with timestamps in the given range (using IQR)" });
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
      message: "Non-anomalous individual order durations for all chain combinations (in seconds, using IQR)",
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
    WITH durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        create_order_id,
        created_at,
        CASE WHEN user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_init - created_at)), 0) END AS user_init_duration,
        CASE WHEN cobi_init IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_init - user_init)), 0) END AS cobi_init_duration,
        CASE WHEN user_redeem IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_redeem - cobi_init)), 0) END AS user_redeem_duration,
        CASE WHEN cobi_redeem IS NOT NULL AND user_redeem IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_redeem - user_redeem)), 0) END AS cobi_redeem_duration,
        CASE WHEN user_refund IS NOT NULL AND user_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (user_refund - user_init)), 0) END AS user_refund_duration,
        CASE WHEN cobi_refund IS NOT NULL AND cobi_init IS NOT NULL THEN GREATEST(EXTRACT(EPOCH FROM (cobi_refund - cobi_init)), 0) END AS cobi_refund_duration
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
    ),
    counts AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        COUNT(user_init_duration) AS user_init_count,
        COUNT(cobi_init_duration) AS cobi_init_count,
        COUNT(user_redeem_duration) AS user_redeem_count,
        COUNT(cobi_redeem_duration) AS cobi_redeem_count
      FROM durations
      GROUP BY source_chain, destination_chain
    ),
    ordered_durations AS MATERIALIZED (
      SELECT
        source_chain,
        destination_chain,
        user_init_duration,
        cobi_init_duration,
        user_redeem_duration,
        cobi_redeem_duration,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_init_duration) AS user_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_init_duration) AS cobi_init_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY user_redeem_duration) AS user_redeem_rn,
        ROW_NUMBER() OVER (PARTITION BY source_chain, destination_chain ORDER BY cobi_redeem_duration) AS cobi_redeem_rn
      FROM durations
      WHERE user_init_duration IS NOT NULL
        OR cobi_init_duration IS NOT NULL
        OR user_redeem_duration IS NOT NULL
        OR cobi_redeem_duration IS NOT NULL
    ),
    stats AS MATERIALIZED (
      SELECT
        c.source_chain,
        c.destination_chain,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.25 * (c.user_init_count + 1))),
               ((CEIL(0.25 * (c.user_init_count + 1)) - (0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.25 * (c.user_init_count + 1)), 1)) +
                ((0.25 * (c.user_init_count + 1)) - FLOOR(0.25 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.25 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_init_duration,
        (SELECT CASE
           WHEN c.user_init_count > 0 THEN
             COALESCE(
               (SELECT user_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_init_rn = FLOOR(0.75 * (c.user_init_count + 1))),
               ((CEIL(0.75 * (c.user_init_count + 1)) - (0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(FLOOR(0.75 * (c.user_init_count + 1)), 1)) +
                ((0.75 * (c.user_init_count + 1)) - FLOOR(0.75 * (c.user_init_count + 1))) * 
                (SELECT user_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_init_rn = GREATEST(CEIL(0.75 * (c.user_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.25 * (c.cobi_init_count + 1))),
               ((CEIL(0.25 * (c.cobi_init_count + 1)) - (0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.25 * (c.cobi_init_count + 1)), 1)) +
                ((0.25 * (c.cobi_init_count + 1)) - FLOOR(0.25 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.25 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_init_duration,
        (SELECT CASE
           WHEN c.cobi_init_count > 0 THEN
             COALESCE(
               (SELECT cobi_init_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_init_rn = FLOOR(0.75 * (c.cobi_init_count + 1))),
               ((CEIL(0.75 * (c.cobi_init_count + 1)) - (0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(FLOOR(0.75 * (c.cobi_init_count + 1)), 1)) +
                ((0.75 * (c.cobi_init_count + 1)) - FLOOR(0.75 * (c.cobi_init_count + 1))) * 
                (SELECT cobi_init_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_init_rn = GREATEST(CEIL(0.75 * (c.cobi_init_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_init_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.25 * (c.user_redeem_count + 1))),
               ((CEIL(0.25 * (c.user_redeem_count + 1)) - (0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.25 * (c.user_redeem_count + 1)), 1)) +
                ((0.25 * (c.user_redeem_count + 1)) - FLOOR(0.25 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.25 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_user_redeem_duration,
        (SELECT CASE
           WHEN c.user_redeem_count > 0 THEN
             COALESCE(
               (SELECT user_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.user_redeem_rn = FLOOR(0.75 * (c.user_redeem_count + 1))),
               ((CEIL(0.75 * (c.user_redeem_count + 1)) - (0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(FLOOR(0.75 * (c.user_redeem_count + 1)), 1)) +
                ((0.75 * (c.user_redeem_count + 1)) - FLOOR(0.75 * (c.user_redeem_count + 1))) * 
                (SELECT user_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.user_redeem_rn = GREATEST(CEIL(0.75 * (c.user_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_user_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.25 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.25 * (c.cobi_redeem_count + 1)) - (0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.25 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.25 * (c.cobi_redeem_count + 1)) - FLOOR(0.25 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.25 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q1_cobi_redeem_duration,
        (SELECT CASE
           WHEN c.cobi_redeem_count > 0 THEN
             COALESCE(
               (SELECT cobi_redeem_duration 
                FROM ordered_durations od 
                WHERE od.source_chain = c.source_chain 
                AND od.destination_chain = c.destination_chain 
                AND od.cobi_redeem_rn = FLOOR(0.75 * (c.cobi_redeem_count + 1))),
               ((CEIL(0.75 * (c.cobi_redeem_count + 1)) - (0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(FLOOR(0.75 * (c.cobi_redeem_count + 1)), 1)) +
                ((0.75 * (c.cobi_redeem_count + 1)) - FLOOR(0.75 * (c.cobi_redeem_count + 1))) * 
                (SELECT cobi_redeem_duration 
                 FROM ordered_durations od 
                 WHERE od.source_chain = c.source_chain 
                 AND od.destination_chain = c.destination_chain 
                 AND od.cobi_redeem_rn = GREATEST(CEIL(0.75 * (c.cobi_redeem_count + 1)), 1)))
             )
           ELSE NULL
         END) AS q3_cobi_redeem_duration
      FROM counts c
    )
    SELECT
      d.source_chain,
      d.destination_chain,
      d.create_order_id,
      d.created_at,
      d.user_init_duration,
      d.cobi_init_duration,
      d.user_redeem_duration,
      d.user_refund_duration,
      d.cobi_redeem_duration,
      d.cobi_refund_duration,
      (
        COALESCE(d.user_init_duration, 0) +
        COALESCE(d.cobi_init_duration, 0) +
        COALESCE(d.user_redeem_duration, 0) +
        COALESCE(d.user_refund_duration, 0) +
        COALESCE(d.cobi_redeem_duration, 0) +
        COALESCE(d.cobi_refund_duration, 0)
      ) AS overall_duration,
      s.q1_user_init_duration,
      s.q3_user_init_duration,
      s.q1_cobi_init_duration,
      s.q3_cobi_init_duration,
      s.q1_user_redeem_duration,
      s.q3_user_redeem_duration,
      s.q1_cobi_redeem_duration,
      s.q3_cobi_redeem_duration
    FROM durations d
    JOIN stats s ON d.source_chain = s.source_chain AND d.destination_chain = s.destination_chain
    WHERE (
        (d.user_init_duration IS NOT NULL AND d.user_init_duration > 
          (s.q3_user_init_duration + 1.5 * (s.q3_user_init_duration - s.q1_user_init_duration)))
        OR (d.cobi_init_duration IS NOT NULL AND d.cobi_init_duration > 
          (s.q3_cobi_init_duration + 1.5 * (s.q3_cobi_init_duration - s.q1_cobi_init_duration)))
        OR (d.user_redeem_duration IS NOT NULL AND d.user_redeem_duration > 
          (s.q3_user_redeem_duration + 1.5 * (s.q3_user_redeem_duration - s.q1_user_redeem_duration)))
        OR (d.cobi_redeem_duration IS NOT NULL AND d.cobi_redeem_duration > 
          (s.q3_cobi_redeem_duration + 1.5 * (s.q3_cobi_redeem_duration - s.q1_cobi_redeem_duration)))
      )
    ORDER BY d.source_chain, d.destination_chain, d.created_at ASC;
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains,
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: "No anomalous orders found with timestamps in the given range (using IQR)" });
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
        thresholds: {
          user_init_duration: {
            lower: order.q1_user_init_duration && order.q3_user_init_duration
              ? parseFloat(order.q1_user_init_duration) - 1.5 * (parseFloat(order.q3_user_init_duration) - parseFloat(order.q1_user_init_duration))
              : null,
            upper: order.q1_user_init_duration && order.q3_user_init_duration
              ? parseFloat(order.q3_user_init_duration) + 1.5 * (parseFloat(order.q3_user_init_duration) - parseFloat(order.q1_user_init_duration))
              : null,
          },
          cobi_init_duration: {
            lower: order.q1_cobi_init_duration && order.q3_cobi_init_duration
              ? parseFloat(order.q1_cobi_init_duration) - 1.5 * (parseFloat(order.q3_cobi_init_duration) - parseFloat(order.q1_cobi_init_duration))
              : null,
            upper: order.q1_cobi_init_duration && order.q3_cobi_init_duration
              ? parseFloat(order.q3_cobi_init_duration) + 1.5 * (parseFloat(order.q3_cobi_init_duration) - parseFloat(order.q1_cobi_init_duration))
              : null,
          },
          user_redeem_duration: {
            lower: order.q1_user_redeem_duration && order.q3_user_redeem_duration
              ? parseFloat(order.q1_user_redeem_duration) - 1.5 * (parseFloat(order.q3_user_redeem_duration) - parseFloat(order.q1_user_redeem_duration))
              : null,
            upper: order.q1_user_redeem_duration && order.q3_user_redeem_duration
              ? parseFloat(order.q3_user_redeem_duration) + 1.5 * (parseFloat(order.q3_user_redeem_duration) - parseFloat(order.q1_user_redeem_duration))
              : null,
          },
          cobi_redeem_duration: {
            lower: order.q1_cobi_redeem_duration && order.q3_cobi_redeem_duration
              ? parseFloat(order.q1_cobi_redeem_duration) - 1.5 * (parseFloat(order.q3_cobi_redeem_duration) - parseFloat(order.q1_cobi_redeem_duration))
              : null,
            upper: order.q1_cobi_redeem_duration && order.q3_cobi_redeem_duration
              ? parseFloat(order.q3_cobi_redeem_duration) + 1.5 * (parseFloat(order.q3_cobi_redeem_duration) - parseFloat(order.q1_cobi_redeem_duration))
              : null,
          },
        },
      });
      return acc;
    }, {});

    res.json({
      message: "Anomalous individual order durations for all chain combinations (in seconds, using IQR)",
      orders: ordersByChain,
    });
  } catch (err: any) {
    console.error("Error fetching anomalous orders:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
};