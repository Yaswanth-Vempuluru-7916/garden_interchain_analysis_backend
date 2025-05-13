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
  "RPC_URL_CITREA",
  "RPC_URL_STARKNET",
  "RPC_URL_MONAD",
  "RPC_URL_HYPERLIQUID",
  "RPC_URL_BITCOIN",
  "RPC_URL_BERA",
  "SUPPORTED_CHAINS",
];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.error("Missing environment variables:", missingEnvVars.join(", "));
  process.exit(1);
}

const alchemyInstances = {
  ethereum_sepolia: new Alchemy({ apiKey: process.env.ALCHEMY_TOKEN, network: Network.ETH_SEPOLIA }),
  base_sepolia: new Alchemy({ apiKey: process.env.ALCHEMY_TOKEN, network: Network.BASE_SEPOLIA }),
  bera_testnet: new Alchemy({
    apiKey: process.env.ALCHEMY_TOKEN,
    url: `${process.env.RPC_URL_BERA}/${process.env.ALCHEMY_TOKEN}`,
  }),
};

// const citreaProvider = new ethers.JsonRpcProvider(process.env.RPC_URL_CITREA);

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

// interface MonadRpcResponse {
//   jsonrpc: string;
//   id: number;
//   result?: {
//     timestamp: string;
//   };
//   error?: {
//     code: number;
//     message: string;
//   };
// }

const getTimestampForBlock = async (chain: string, blockNumber: number | null): Promise<number | null> => {
  if (!blockNumber) {
    console.warn(`Block number is null for chain ${chain}. Cannot fetch timestamp.`);
    return null;
  }

  let rpcChain = chain;
  if (["arbitrum", "ethereum"].includes(chain)) {
    rpcChain = "ethereum";
  }

  if (["ethereum", "base", "bera"].includes(rpcChain)) {
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

  // if (rpcChain === "monad_testnet") {
  //   try {
  //     if (isNaN(Number(blockNumber))) {
  //       console.error(`Invalid block number: ${blockNumber}`);
  //       return null;
  //     }
  //     const rpcUrl = `${process.env.RPC_URL_MONAD}${process.env.ALCHEMY_TOKEN}`;
  //     const hexBlockNumber = "0x" + Number(blockNumber).toString(16);
  //     const payload = {
  //       jsonrpc: "2.0",
  //       method: "eth_getBlockByNumber",
  //       params: [hexBlockNumber, false],
  //       id: 1,
  //     };
  //     const res = await fetch(rpcUrl, {
  //       method: "POST",
  //       headers: { "Content-Type": "application/json" },
  //       body: JSON.stringify(payload),
  //     });
  //     if (!res.ok) {
  //       console.error(`HTTP Error: ${res.status} ${res.statusText}`);
  //       return null;
  //     }
  //     const data = (await res.json()) as MonadRpcResponse;
  //     if (data.error) {
  //       console.log(`RPC Error: ${data.error.message} (Code: ${data.error.code})`);
  //       return null;
  //     }
  //     if (data.result && data.result.timestamp) {
  //       return parseInt(data.result.timestamp, 16);
  //     }
  //     console.log(`Block ${blockNumber} not found for chain ${rpcChain}`);
  //     return null;
  //   } catch (err: any) {
  //     console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, err.message);
  //     return null;
  //   }
  // }

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

  // if (rpcChain === "citrea_testnet") {
  //   try {
  //     const block = await citreaProvider.getBlock(Number(blockNumber));
  //     if (block && block.timestamp) {
  //       return block.timestamp;
  //     }
  //     console.log(`Block ${blockNumber} not found for chain ${rpcChain}`);
  //     return null;
  //   } catch (err: any) {
  //     console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, err.message);
  //     return null;
  //   }
  // }

  if (rpcChain === "bitcoin") {
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

      console.log(`Block ${blockNumber} not found in the last 15 blocks for chain ${rpcChain}`);
      return null;
    } catch (err) {
      console.error(`Error fetching block ${blockNumber} for chain ${rpcChain}:`, (err as Error).message);
      return null;
    }
  }

  console.log(`Chain ${rpcChain} not supported for timestamp fetching`);
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
             user_init_block_number, cobi_init_block_number,
             user_redeem_block_number, user_refund_block_number,
             cobi_redeem_block_number, cobi_refund_block_number,
             create_order_id
      FROM ${ORDERS_TABLE}
      WHERE create_order_id = ANY($1)
        AND (
          (user_init_block_number IS NOT NULL AND user_init IS NULL) OR
          (user_redeem_block_number IS NOT NULL AND user_redeem IS NULL) OR
          (user_refund_block_number IS NOT NULL AND user_refund IS NULL) OR
          (cobi_init_block_number IS NOT NULL AND cobi_init IS NULL) OR
          (cobi_redeem_block_number IS NOT NULL AND cobi_redeem IS NULL) OR
          (cobi_refund_block_number IS NOT NULL AND cobi_refund IS NULL)
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
          if (user_init_block_number && !userInitTimestamp) {
            timestampPromises.push(
              getTimestampForBlock(source_chain, user_init_block_number).then((ts) => {
                userInitTimestamp = ts ? formatTimestampToIST(ts) : null;
              })
            );
          }
          if (user_redeem_block_number && !userRedeemTimestamp) {
            timestampPromises.push(
              getTimestampForBlock(source_chain, user_redeem_block_number).then((ts) => {
                userRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
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
        } else {
          console.warn(`Source chain ${source_chain} not supported for timestamp fetching for order ${create_order_id}`);
        }

        if (supportedChains.includes(destination_chain)) {
          if (cobi_init_block_number && !cobiInitTimestamp) {
            timestampPromises.push(
              getTimestampForBlock(destination_chain, cobi_init_block_number).then((ts) => {
                cobiInitTimestamp = ts ? formatTimestampToIST(ts) : null;
              })
            );
          }
          if (cobi_redeem_block_number && !cobiRedeemTimestamp) {
            timestampPromises.push(
              getTimestampForBlock(destination_chain, cobi_redeem_block_number).then((ts) => {
                cobiRedeemTimestamp = ts ? formatTimestampToIST(ts) : null;
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
        } else {
          console.warn(
            `Destination chain ${destination_chain} not supported for timestamp fetching for order ${create_order_id}`
          );
        }

        await Promise.all(timestampPromises);

        const hasChanges =
          (user_init_block_number && !user_init && userInitTimestamp) ||
          (cobi_init_block_number && !cobi_init && cobiInitTimestamp) ||
          (user_redeem_block_number && !user_redeem && userRedeemTimestamp) ||
          (user_refund_block_number && !user_refund && userRefundTimestamp) ||
          (cobi_redeem_block_number && !cobi_redeem && cobiRedeemTimestamp) ||
          (cobi_refund_block_number && !cobi_refund && cobiRefundTimestamp);

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