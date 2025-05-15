import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

export const mainPool = new Pool({
  user: process.env.MAIN_DB_USER,
  host: process.env.MAIN_DB_HOST,
  database: process.env.MAIN_DB_NAME,
  password: process.env.MAIN_DB_PASSWORD,
  port: Number(process.env.MAIN_DB_PORT),
});

export const analysisPool = new Pool({
  user: process.env.ANALYSIS_DB_USER,
  host: process.env.ANALYSIS_DB_HOST,
  database: process.env.ANALYSIS_DB_NAME,
  password: process.env.ANALYSIS_DB_PASSWORD,
  port: Number(process.env.ANALYSIS_DB_PORT),
});

export const supportedChains = [
  "arbitrum",
  "base",
  "bitcoin",
  "ethereum",
  "hyperliquid",
  "starknet",
  "bera",
];

export const ORDERS_TABLE = "orders_3"