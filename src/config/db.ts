import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

export const stagePool = new Pool({
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

export const supportedChains: string[] = (() => {
  const chains = process.env.SUPPORTED_CHAINS;
  if (!chains) {
    throw new Error("SUPPORTED_CHAINS is not defined in environment variables");
  }
  try {
    const parsed = JSON.parse(chains);
    if (!Array.isArray(parsed) || !parsed.every((chain) => typeof chain === "string")) {
      throw new Error("SUPPORTED_CHAINS must be an array of strings");
    }
    return parsed;
  } catch (error: any) {
    throw new Error(`Failed to parse SUPPORTED_CHAINS: ${error.message}`);
  }
})();

export const ORDERS_TABLE = "orders_analysis"