import { Pool } from "pg";
import { Alchemy, Network } from "alchemy-sdk";
import axios from "axios";
import { ethers } from "ethers";
import dotenv from "dotenv";
import { analysisPool, ORDERS_TABLE, supportedChains } from "../config/db";
import { formatTimestampToIST } from "../utils/timeUtils";

dotenv.config();

const requiredEnvVars = [
  "ANALYSIS_DB_USER",
  "ANALYSIS_DB_HOST",
  "ANALYSIS_DB_NAME",
  "ANALYSIS_DB_PASSWORD",
  "ANALYSIS_DB_PORT",
  "ALCHEMY_TOKEN",
  "RPC_URL_STARKNET",
  "RPC_URL_HYPERLIQUID",
  "RPC_URL_BITCOIN",
  "RPC_URL_BERA",
  "RPC_URL_BITCOIN_TX",
  "RPC_URL_UNICHAIN"
];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.error("Missing environment variables:", missingEnvVars.join(", "));
  process.exit(1);
}

const alchemyInstances = {
  ethereum: new Alchemy({ apiKey: process.env.ALCHEMY_TOKEN, network: Network.ETH_MAINNET }),
  base: new Alchemy({ apiKey: process.env.ALCHEMY_TOKEN, network: Network.BASE_MAINNET }),
  bera: new Alchemy({
    apiKey: process.env.ALCHEMY_TOKEN,
    url: `${process.env.RPC_URL_BERA}/${process.env.ALCHEMY_TOKEN}`,
  }),
  unichain: new Alchemy({
    apiKey: process.env.ALCHEMY_TOKEN,
    url: `${process.env.RPC_URL_UNICHAIN}/${process.env.ALCHEMY_TOKEN}`,
  }),
  arbitrum: new Alchemy({ apiKey: process.env.ALCHEMY_TOKEN, network: Network.ARB_MAINNET }),
};

interface StarkNetRpcResponse {
  jsonrpc: string;
  id: number;
  result?: {
    timestamp: number;
  };
  error?: {
    code: number;
    message: string;
  };
}

interface BlockInfo {
  height: number;
  timestamp: number;
}

// Utility function for delaying execution
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Fetch timestamp for block number (for chains other than Ethereum, Arbitrum, Bitcoin)
const getTimestampForBlock = async (chain: string, blockNumber: number | null): Promise<number | null> => {
  if (!blockNumber) {
    console.warn(`Block number is null for chain ${chain}. Cannot fetch timestamp.`);
    return null;
  }

  let rpcChain = chain;

  if (["base", "bera","unichain"].includes(rpcChain)) {
    try {
      const alchemy = alchemyInstances[rpcChain as keyof typeof alchemyInstances];
      const block = await alchemy.core.getBlock(Number(blockNumber));
      if (block && block.timestamp) {
        return block.timestamp;
      }
      console.log(`Block ${blockNumber} not found for chain ${rpcChain}`);
      return null;
    } catch (err: any) {
      console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, err.message);
      return null;
    }
  }

  if (rpcChain === "starknet") {
    try {
      const rpcUrl = `${process.env.RPC_URL_STARKNET}${process.env.ALCHEMY_TOKEN}`;
      const payload = {
        jsonrpc: "2.0",
        id: 1,
        method: "starknet_getBlockWithTxs",
        params: [{ block_number: Number(blockNumber) }],
      };
      const res = await fetch(rpcUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = (await res.json()) as StarkNetRpcResponse;
      if (data.error) {
        console.log(`RPC Error: ${data.error.message} (Code: ${data.error.code})`);
        return null;
      }
      if (data.result && data.result.timestamp) {
        return data.result.timestamp;
      }
      console.log(`Block ${blockNumber} not found for chain ${rpcChain}`);
      return null;
    } catch (err: any) {
      console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, err.message);
      return null;
    }
  }

  if (rpcChain === "hyperliquid") {
    try {
      const rpcUrl = process.env.RPC_URL_HYPERLIQUID;
      if (!rpcUrl) {
        throw new Error("RPC_URL_HYPERLIQUID is not defined in .env");
      }
      const params = [`0x${Number(blockNumber).toString(16)}`, false];
      const response = await axios.post(rpcUrl, {
        jsonrpc: "2.0",
        method: "eth_getBlockByNumber",
        params: params,
        id: 1,
      });
      const block = response.data.result;
      if (block && block.timestamp) {
        return parseInt(block.timestamp, 16);
      }
      console.log(`Block ${blockNumber} not found for chain ${rpcChain}`);
      return null;
    } catch (err: any) {
      console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, err.message);
      return null;
    }
  }

  console.log(`Chain ${rpcChain} not supported for block-based timestamp fetching`);
  return null;
};

// Fetch timestamp for transaction hash (for Ethereum, Arbitrum, Bitcoin)
const getTimestampForTransaction = async (
  chain: string,
  txHash: string | null,
  blockNumber: number | null,
  create_order_id: string,
  field: string
): Promise<number | null> => {
  if (!txHash) {
    console.warn(`Transaction hash is null for chain ${chain}, order ${create_order_id}, field=${field}. Cannot fetch timestamp.`);
    return null;
  }

  let rpcChain = chain;

  if (["ethereum", "arbitrum"].includes(rpcChain)) {
    try {
      const alchemy = alchemyInstances[rpcChain as keyof typeof alchemyInstances];
      const tx = await alchemy.core.getTransactionReceipt(txHash);
      if (tx && tx.blockNumber) {
        const block = await alchemy.core.getBlock(tx.blockNumber);
        if (block && block.timestamp) {
          return block.timestamp;
        }
      }
      console.log(`Transaction ${txHash} not found for chain ${rpcChain}, order ${create_order_id}, field=${field}`);
      return null;
    } catch (err: any) {
      console.error(`Error fetching transaction ${txHash} for chain ${rpcChain}, order ${create_order_id}, field=${field}:`, err.message);
      return null;
    }
  }

  if (rpcChain === "bitcoin") {
    // First, try fetching timestamp using transaction hash
    try {
      const cleanTxHash = txHash.split(":")[0];
      const apiUrl = `${process.env.RPC_URL_BITCOIN_TX}/${cleanTxHash}`;
      const maxRetries = 5;
      let attempt = 0;

      while (attempt < maxRetries) {
        const response = await fetch(apiUrl);
        if (!response.ok) {
          if (response.status === 429) {
            attempt++;
            const backoff = Math.pow(2, attempt) * 1000; // Exponential backoff: 1s, 2s, 4s
            console.warn(
              `Rate limit hit for transaction ${txHash} on ${rpcChain} for order ${create_order_id}, field=${field}. ` +
              `Retrying (${attempt}/${maxRetries}) after ${backoff}ms`
            );
            await delay(backoff);
            continue;
          }
          throw new Error(`HTTP error! Status: ${response.status}`);
        }
        const txData = await response.json();
        if (txData.status && txData.status.block_time) {
          return txData.status.block_time; // Already in seconds
        }
        console.log(`Transaction ${txHash} is unconfirmed on ${rpcChain} for order ${create_order_id}, field=${field}`);
        break; // Exit retry loop if transaction is unconfirmed
      }
    } catch (err: any) {
      console.error(`Error fetching Bitcoin transaction ${txHash} for order ${create_order_id}, field=${field}:`, err.message);
    }

    // Fallback to block number if txHash fetching fails or returns no timestamp
    console.log(`Falling back to block number ${blockNumber} for chain ${rpcChain}, order ${create_order_id}, field=${field}`);
    if (!blockNumber) {
      console.warn(`Block number is null for chain ${rpcChain}, order ${create_order_id}, field=${field}. Cannot fetch timestamp via fallback.`);
      return null;
    }
    try {
      const baseUrl = process.env.RPC_URL_BITCOIN;
      if (!baseUrl) {
        throw new Error("RPC_URL_BITCOIN is not defined in .env");
      }
      const response = await fetch(`${baseUrl}/${blockNumber}`);
      const data = (await response.json()) as BlockInfo[];
      const block = data.find((b) => b.height === Number(blockNumber));
      if (block?.timestamp) {
        return block.timestamp;
      }
      console.log(`Block ${blockNumber} not found in the last 15 blocks for chain ${rpcChain}, order ${create_order_id}, field=${field}`);
      return null;
    } catch (err: any) {
      console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}, order ${create_order_id}, field=${field}:`, err.message);
      return null;
    }
  }

  console.log(`Chain ${rpcChain} not supported for transaction-based timestamp fetching, order ${create_order_id}, field=${field}`);
  return null;
};

export const updateTimestampsForOrders = async (orderIds: string[]): Promise<void> => {
  if (!orderIds || orderIds.length === 0) {
    console.log("No order IDs provided for timestamp update");
    return;
  }

  try {
    const query = `
      SELECT id, source_chain, destination_chain,
             user_init, cobi_init,
             user_redeem, cobi_redeem,
             user_refund, cobi_refund,
             user_init_tx_hash, cobi_init_tx_hash,
             user_redeem_tx_hash, user_refund_tx_hash,
             cobi_redeem_tx_hash, cobi_refund_tx_hash,
             user_init_block_number, cobi_init_block_number,
             user_redeem_block_number, user_refund_block_number,
             cobi_redeem_block_number, cobi_refund_block_number,
             create_order_id
      FROM ${ORDERS_TABLE}
      WHERE create_order_id = ANY($1)
        AND (
          (user_init_tx_hash IS NOT NULL AND user_init IS NULL AND source_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (user_redeem_tx_hash IS NOT NULL AND user_redeem IS NULL AND destination_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (user_refund_tx_hash IS NOT NULL AND user_refund IS NULL AND source_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_init_tx_hash IS NOT NULL AND cobi_init IS NULL AND destination_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_redeem_tx_hash IS NOT NULL AND cobi_redeem IS NULL AND source_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_refund_tx_hash IS NOT NULL AND cobi_refund IS NULL AND destination_chain IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (user_init_block_number IS NOT NULL AND user_init IS NULL AND source_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (user_redeem_block_number IS NOT NULL AND user_redeem IS NULL AND destination_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (user_refund_block_number IS NOT NULL AND user_refund IS NULL AND source_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_init_block_number IS NOT NULL AND cobi_init IS NULL AND destination_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_redeem_block_number IS NOT NULL AND cobi_redeem IS NULL AND source_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin')) OR
          (cobi_refund_block_number IS NOT NULL AND cobi_refund IS NULL AND destination_chain NOT IN ('ethereum', 'arbitrum', 'bitcoin'))
        )
    `;
    const result = await analysisPool.query(query, [orderIds]);
    console.log(`Found ${result.rowCount} rows in ${ORDERS_TABLE} to update timestamps`);

    let successfulUpdates = 0;
    let failedUpdates: string[] = [];

    const chunkSize = 5;
    for (let i = 0; i < result.rows.length; i += chunkSize) {
      const chunk = result.rows.slice(i, i + chunkSize);
      const updatePromises = chunk.map(async (row: any) => {
        const {
          id,
          source_chain,
          destination_chain,
          user_init,
          cobi_init,
          user_redeem,
          cobi_redeem,
          user_refund,
          cobi_refund,
          user_init_tx_hash,
          cobi_init_tx_hash,
          user_redeem_tx_hash,
          user_refund_tx_hash,
          cobi_redeem_tx_hash,
          cobi_refund_tx_hash,
          user_init_block_number,
          cobi_init_block_number,
          user_redeem_block_number,
          user_refund_block_number,
          cobi_redeem_block_number,
          cobi_refund_block_number,
          create_order_id,
        } = row;

        let userInitTimestamp = user_init;
        let cobiInitTimestamp = cobi_init;
        let userRedeemTimestamp = user_redeem;
        let userRefundTimestamp = user_refund;
        let cobiRedeemTimestamp = cobi_redeem;
        let cobiRefundTimestamp = cobi_refund;

        const timestampPromises: Promise<void>[] = [];

        if (supportedChains.includes(source_chain)) {
          if (source_chain === "ethereum" || source_chain === "arbitrum" || source_chain === "bitcoin") {
            if (user_init_tx_hash && !userInitTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(source_chain, user_init_tx_hash, user_init_block_number, create_order_id, "user_init_tx_hash").then((ts) => {
                  userInitTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (user_refund_tx_hash && !userRefundTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(source_chain, user_refund_tx_hash, user_refund_block_number, create_order_id, "user_refund_tx_hash").then((ts) => {
                  userRefundTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (cobi_redeem_tx_hash && !cobiRedeemTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(source_chain, cobi_redeem_tx_hash, cobi_redeem_block_number, create_order_id, "cobi_redeem_tx_hash").then((ts) => {
                  cobiRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
          } else {
            if (user_init_block_number && !userInitTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(source_chain, user_init_block_number).then((ts) => {
                  userInitTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (user_refund_block_number && !userRefundTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(source_chain, user_refund_block_number).then((ts) => {
                  userRefundTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (cobi_redeem_block_number && !cobiRedeemTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(source_chain, cobi_redeem_block_number).then((ts) => {
                  cobiRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
          }
        } else {
          console.warn(`Source chain ${source_chain} not supported for timestamp fetching for order ${create_order_id}`);
        }

        if (supportedChains.includes(destination_chain)) {
          if (destination_chain === "ethereum" || destination_chain === "arbitrum" || destination_chain === "bitcoin") {
            if (cobi_init_tx_hash && !cobiInitTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(destination_chain, cobi_init_tx_hash, cobi_init_block_number, create_order_id, "cobi_init_tx_hash").then((ts) => {
                  cobiInitTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (user_redeem_tx_hash && !userRedeemTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(destination_chain, user_redeem_tx_hash, user_redeem_block_number, create_order_id, "user_redeem_tx_hash").then((ts) => {
                  userRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (cobi_refund_tx_hash && !cobiRefundTimestamp) {
              timestampPromises.push(
                getTimestampForTransaction(destination_chain, cobi_refund_tx_hash, cobi_refund_block_number, create_order_id, "cobi_refund_tx_hash").then((ts) => {
                  cobiRefundTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
          } else {
            if (cobi_init_block_number && !cobiInitTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(destination_chain, cobi_init_block_number).then((ts) => {
                  cobiInitTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (user_redeem_block_number && !userRedeemTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(destination_chain, user_redeem_block_number).then((ts) => {
                  userRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
            if (cobi_refund_block_number && !cobiRefundTimestamp) {
              timestampPromises.push(
                getTimestampForBlock(destination_chain, cobi_refund_block_number).then((ts) => {
                  cobiRefundTimestamp = ts ? formatTimestampToIST(ts) : null;
                })
              );
            }
          }
        } else {
          console.warn(
            `Destination chain ${destination_chain} not supported for timestamp fetching for order ${create_order_id}`
          );
        }

        await Promise.all(timestampPromises);

        const hasChanges =
          (user_init_tx_hash && !user_init && userInitTimestamp && (source_chain === "ethereum" || source_chain === "arbitrum" || source_chain === "bitcoin")) ||
          (cobi_init_tx_hash && !cobi_init && cobiInitTimestamp && (destination_chain === "ethereum" || destination_chain === "arbitrum" || destination_chain === "bitcoin")) ||
          (user_redeem_tx_hash && !user_redeem && userRedeemTimestamp && (destination_chain === "ethereum" || destination_chain === "arbitrum" || destination_chain === "bitcoin")) ||
          (user_refund_tx_hash && !user_refund && userRefundTimestamp && (source_chain === "ethereum" || source_chain === "arbitrum" || source_chain === "bitcoin")) ||
          (cobi_redeem_tx_hash && !cobi_redeem && cobiRedeemTimestamp && (source_chain === "ethereum" || source_chain === "arbitrum" || source_chain === "bitcoin")) ||
          (cobi_refund_tx_hash && !cobi_refund && cobiRefundTimestamp && (destination_chain === "ethereum" || destination_chain === "arbitrum" || destination_chain === "bitcoin")) ||
          (user_init_block_number && !user_init && userInitTimestamp && source_chain !== "ethereum" && source_chain !== "arbitrum" && source_chain !== "bitcoin") ||
          (cobi_init_block_number && !cobi_init && cobiInitTimestamp && destination_chain !== "ethereum" && destination_chain !== "arbitrum" && destination_chain !== "bitcoin") ||
          (user_redeem_block_number && !user_redeem && userRedeemTimestamp && destination_chain !== "ethereum" && destination_chain !== "arbitrum" && destination_chain !== "bitcoin") ||
          (user_refund_block_number && !user_refund && userRefundTimestamp && source_chain !== "ethereum" && source_chain !== "arbitrum" && source_chain !== "bitcoin") ||
          (cobi_redeem_block_number && !cobi_redeem && cobiRedeemTimestamp && source_chain !== "ethereum" && source_chain !== "arbitrum" && source_chain !== "bitcoin") ||
          (cobi_refund_block_number && !cobi_refund && cobiRefundTimestamp && destination_chain !== "ethereum" && destination_chain !== "arbitrum" && destination_chain !== "bitcoin");

        if (hasChanges) {
          const updateQuery = `
            UPDATE ${ORDERS_TABLE}
            SET user_init = $1::timestamp with time zone,
                cobi_init = $2::timestamp with time zone,
                user_redeem = $3::timestamp with time zone,
                user_refund = $4::timestamp with time zone,
                cobi_redeem = $5::timestamp with time zone,
                cobi_refund = $6::timestamp with time zone
            WHERE id = $7
          `;
          await analysisPool.query(updateQuery, [
            userInitTimestamp,
            cobiInitTimestamp,
            userRedeemTimestamp,
            userRefundTimestamp,
            cobiRedeemTimestamp,
            cobiRefundTimestamp,
            id,
          ]);
          console.log(
            `Updated timestamps for order ${create_order_id} (id=${id}): user_init=${userInitTimestamp}, cobi_init=${cobiInitTimestamp}, user_redeem=${userRedeemTimestamp}, user_refund=${userRefundTimestamp}, cobi_redeem=${cobiRedeemTimestamp}, cobi_refund=${cobiRefundTimestamp}`
          );
          successfulUpdates++;
        } else {
          console.warn(
            `No timestamps updated for order ${create_order_id} (id=${id}): all timestamps are either already set or could not be fetched.`
          );
          failedUpdates.push(create_order_id);
        }
      });

      await Promise.all(updatePromises);
    }

    console.log(
      `Timestamp update completed: ${successfulUpdates} orders updated successfully, ${failedUpdates.length} orders failed.`
    );
    if (failedUpdates.length > 0) {
      console.log(`Failed orders: ${failedUpdates.join(", ")}`);
    }
  } catch (err) {
    console.error(`Error updating timestamps in ${ORDERS_TABLE}:`, err);
    throw err;
  }
};