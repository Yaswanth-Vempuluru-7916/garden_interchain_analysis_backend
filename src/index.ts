import express, { Request, Response, NextFunction } from "express";
import { Pool } from "pg";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// Pool for stage_db (source data)
const stagePool = new Pool({
  user: process.env.MAIN_DB_USER,
  host: process.env.MAIN_DB_HOST,
  database: process.env.MAIN_DB_NAME,
  password: process.env.MAIN_DB_PASSWORD,
  port: Number(process.env.MAIN_DB_PORT),
});

// Pool for garden_interchain_analysis (storing results)
const analysisPool = new Pool({
  user: process.env.ANALYSIS_DB_USER,
  host: process.env.ANALYSIS_DB_HOST,
  database: process.env.ANALYSIS_DB_NAME,
  password: process.env.ANALYSIS_DB_PASSWORD,
  port: Number(process.env.ANALYSIS_DB_PORT),
});

// Initialize orders_final table
const initTable = async () => {
  const createTableQuery = `
      CREATE TABLE IF NOT EXISTS orders_final (
        id SERIAL PRIMARY KEY,
        create_order_id TEXT NOT NULL UNIQUE,
        source_swap_id TEXT NOT NULL,
        destination_swap_id TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
        source_chain TEXT NOT NULL,
        destination_chain TEXT NOT NULL,
        user_init TIMESTAMP WITH TIME ZONE,
        cobi_init TIMESTAMP WITH TIME ZONE,
        user_redeem TIMESTAMP WITH TIME ZONE,
        cobi_redeem TIMESTAMP WITH TIME ZONE,
        user_refund TIMESTAMP WITH TIME ZONE,
        cobi_refund TIMESTAMP WITH TIME ZONE,
        secret_hash TEXT,
        user_init_block_number BIGINT,
        user_redeem_block_number BIGINT,
        user_refund_block_number BIGINT,
        cobi_init_block_number BIGINT,
        cobi_redeem_block_number BIGINT,
        cobi_refund_block_number BIGINT,
        user_init_tx_hash TEXT,
        user_redeem_tx_hash TEXT,
        user_refund_tx_hash TEXT,
        cobi_init_tx_hash TEXT,
        cobi_redeem_tx_hash TEXT,
        cobi_refund_tx_hash TEXT
    );
    `;
  try {
    await analysisPool.query(createTableQuery);
    console.log(
      "orders_final table ensured with TIMESTAMPTZ columns, secret_hash, block numbers, and chain columns"
    );
  } catch (err) {
    console.error("Failed to ensure orders_final table:", err);
  }
};

// Populate orders_final with new completed orders from stage_db
const populateOrderAnalysis = async () => {
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
        `;

    await stagePool.query("SET TIME ZONE 'UTC'");
    const result = await stagePool.query(query);
   
    if (result.rowCount === 0) return;

    await analysisPool.query("BEGIN");

    for (const row of result.rows) {
      await analysisPool.query(
        `
          INSERT INTO orders_final (
            create_order_id, source_swap_id, destination_swap_id, created_at,
            source_chain, destination_chain,
            user_init, cobi_init, user_redeem, cobi_redeem, user_refund, cobi_refund,
            secret_hash,
            user_init_block_number, user_redeem_block_number, user_refund_block_number,
            cobi_init_block_number, cobi_redeem_block_number, cobi_refund_block_number,
            user_init_tx_hash, user_redeem_tx_hash, user_refund_tx_hash,
            cobi_init_tx_hash, cobi_redeem_tx_hash, cobi_refund_tx_hash
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                  $20, $21, $22, $23, $24, $25)
          ON CONFLICT (create_order_id) DO NOTHING
        `,
        [
          row.create_order_id,
          row.source_swap_id,
          row.destination_swap_id,
          row.created_at,
          row.source_chain,
          row.destination_chain,
          null,
          null,
          null,
          null,
          null,
          null,
          row.secret_hash,
          row.source_init_block_number,
          row.source_redeem_tx_hash ? row.source_redeem_block_number : null,
          row.source_refund_tx_hash ? row.source_refund_block_number : null,
          row.destination_init_block_number,
          row.destination_redeem_tx_hash ? row.destination_redeem_block_number : null,
          row.destination_refund_tx_hash ? row.destination_refund_block_number : null,
          row.source_init_tx_hash ? row.source_init_tx_hash : null,
          row.source_redeem_tx_hash ? row.source_redeem_tx_hash : null,
          row.source_refund_tx_hash ? row.source_refund_tx_hash : null,
          row.destination_init_tx_hash ? row.destination_init_tx_hash : null,
          row.destination_redeem_tx_hash ? row.destination_redeem_tx_hash : null,
          row.destination_refund_tx_hash ? row.destination_refund_tx_hash : null,
        ]
      );
    }

    await analysisPool.query("COMMIT");
    console.log(`Inserted ${result.rowCount} new orders into orders_final`);
  } catch (err) {
    await analysisPool.query("ROLLBACK");
    console.error("Error populating orders_final:", err);
  }
};

// Define the request body interface for timeframe filtering
interface TimeframeRequestBody {
  start_time?: string;
  end_time?: string;
}

// List of supported chains
const supportedChains = [
  'ethereum_sepolia',
  'base_sepolia',
  'starknet_sepolia',
  'monad_testnet',
  'hyperliquid_testnet',
  'citrea_testnet',
  'bitcoin_testnet',
  'arbitrum_sepolia'
];

// Handler to get average durations for all chain combinations and last updated timestamp
const getChainCombinationAverages = async (req: Request<{}, {}, TimeframeRequestBody>, res: Response, next: NextFunction): Promise<void> => {
  const { start_time, end_time } = req.body;

  // Default to last 30 days if timeframe not provided
  const defaultEndTime = new Date().toISOString();
  const defaultStartTime = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const queryStartTime = start_time || defaultStartTime;
  const queryEndTime = end_time || defaultEndTime;

  // Validate date formats
  const isValidISODate = (date: string) => !isNaN(Date.parse(date));
  if (start_time && !isValidISODate(start_time) || end_time && !isValidISODate(end_time)) {
    res.status(400).json({ error: 'Invalid date format' });
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
    FROM orders_final
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
    GROUP BY source_chain, destination_chain
    ORDER BY source_chain, destination_chain;
  `;

  const lastUpdatedQuery = `
    SELECT MAX(GREATEST(
      COALESCE(user_init, '1970-01-01'::timestamp with time zone),
      COALESCE(user_redeem, '1970-01-01'::timestamp with time zone),
      COALESCE(user_refund, '1970-01-01'::timestamp with time zone),
      COALESCE(cobi_init, '1970-01-01'::timestamp with time zone),
      COALESCE(cobi_redeem, '1970-01-01'::timestamp with time zone),
      COALESCE(cobi_refund, '1970-01-01'::timestamp with time zone)
    )) AS last_updated
    FROM orders_final
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
      supportedChains
    ]);

    const lastUpdatedResult = await analysisPool.query(lastUpdatedQuery);
    const lastUpdated = lastUpdatedResult.rows[0]?.last_updated?.toISOString() || '1970-01-01T00:00:00.000Z';

    const averagesByChain = result.rows.reduce((acc: any, row: any) => {
      const key = `${row.source_chain}-${row.destination_chain}`;
      acc[key] = {
        total_orders: parseInt(row.total_orders, 10),
        avg_user_init_duration: row.avg_user_init_duration ? parseFloat(row.avg_user_init_duration) : null,
        avg_cobi_init_duration: parseFloat(row.avg_cobi_init_duration),
        avg_user_redeem_duration: row.avg_user_redeem_duration ? parseFloat(row.avg_user_redeem_duration) : null,
        avg_user_refund_duration: row.avg_user_refund_duration ? parseFloat(row.avg_user_refund_duration) : null,
        avg_cobi_redeem_duration: row.avg_cobi_redeem_duration ? parseFloat(row.avg_cobi_redeem_duration) : null,
        avg_cobi_refund_duration: row.avg_cobi_refund_duration ? parseFloat(row.avg_cobi_refund_duration) : null
      };
      return acc;
    }, {});

    const chainCombinations: any = {};
    supportedChains.forEach(source => {
      supportedChains.forEach(destination => {
        const key = `${source}-${destination}`;
        chainCombinations[key] = averagesByChain[key] || {
          total_orders: 0,
          avg_user_init_duration: null,
          avg_cobi_init_duration: null,
          avg_user_redeem_duration: null,
          avg_user_refund_duration: null,
          avg_cobi_redeem_duration: null,
          avg_cobi_refund_duration: null
        };
      });
    });

    res.json({
      message: 'Average durations for all chain combinations (in seconds)',
      last_updated: lastUpdated,
      averages: chainCombinations
    });
  } catch (err) {
    console.error('Error calculating chain combination averages:', err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

// Handler to get individual orders for all chain combinations in a timeframe
const getAllIndividualOrders = async (req: Request<{}, {}, TimeframeRequestBody>, res: Response, next: NextFunction): Promise<void> => {
  const { start_time, end_time } = req.body;

  const defaultEndTime = new Date().toISOString();
  const defaultStartTime = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const queryStartTime = start_time || defaultStartTime;
  const queryEndTime = end_time || defaultEndTime;

  const isValidISODate = (date: string) => !isNaN(Date.parse(date));
  if (start_time && !isValidISODate(start_time) || end_time && !isValidISODate(end_time)) {
    res.status(400).json({ error: 'Invalid date format' });
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
    FROM orders_final
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
    ORDER BY source_chain, destination_chain, created_at ASC;
  `;

  try {
    const result = await analysisPool.query(query, [
      queryStartTime,
      queryEndTime,
      supportedChains,
      supportedChains
    ]);

    if (result.rows.length === 0) {
      res.status(404).json({ error: 'No orders found with timestamps in the given range' });
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
          overall_duration: order.overall_duration ? parseFloat(order.overall_duration) : null
        }
      });
      return acc;
    }, {});

    res.json({
      message: 'Individual order durations for all chain combinations (in seconds)',
      orders: ordersByChain
    });
  } catch (err) {
    console.error('Error fetching individual orders:', err);
    res.status(500).json({ error: 'Database query failed' });
  }
};

// Handler for syncing orders
const syncOrders = async (req: Request, res: Response): Promise<void> => {
  try {
    await initTable();
    await populateOrderAnalysis();
    res.status(200).json({ message: 'Order sync completed successfully' });
  } catch (err) {
    console.error('Error during order sync:', err);
    res.status(500).json({ error: 'Order sync failed' });
  }
};

// Routes
app.post('/averages', getChainCombinationAverages);
app.post('/orders/all', getAllIndividualOrders);
app.post('/sync', syncOrders);

// Initialize table and start server
const port = process.env.PORT || 3000;
initTable().then(() => {
  app.listen(port, () => {
    console.log(`Backend running on http://localhost:${port}`);
  });
}).catch(err => {
  console.error('Failed to initialize table:', err);
  process.exit(1);
});