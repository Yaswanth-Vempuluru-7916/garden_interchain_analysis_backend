// index.ts
import express, { Express } from "express";
import cors from "cors";
import dotenv from "dotenv";
import { analysisPool } from "./config/db";
import { initTable } from "./services/dbService";
import routes from "./routes";
import cron from "node-cron";
import { performOrderSync, performTimestampUpdate } from "./controllers/syncController";

dotenv.config();

const app: Express = express();
app.use(cors());
app.use(express.json());

async function checkDbConnection() {
  let client;
  try {
    client = await analysisPool.connect();
    console.log(` Database connection successful`);
  } catch (err: any) {
    console.error(` Database connection failed: ${err.message}\nStack: ${err.stack}`);
    throw err;
  } finally {
    if (client) client.release();
  }
}

// Prevent overlapping job executions
let isRunning = false;

async function runSyncAndUpdate() {
  if (isRunning) {
    console.log(`Previous job still running, skipping this run...`);
    return;
  }
  isRunning = true;
  console.log(`Starting sync and timestamp update...`);
  try {
    await checkDbConnection();
    console.log(`Sync started...`);
    await performOrderSync();
    console.log(`Sync completed, proceeding to update timestamps...`);
    await performTimestampUpdate();
    console.log(`Timestamp update completed.`);
  } catch (err: any) {
    console.error(`Error during sync/update: ${err.message}\nStack: ${err.stack}`);
  } finally {
    isRunning = false;
    console.log(`Job finished, isRunning reset to false.`);
  }
}

function scheduleSyncAndUpdate() {

  cron.schedule("0 */2 * * *", async () => {
    await runSyncAndUpdate();
  }, { timezone: "UTC" });

  console.log(`Cron job scheduled to run every 5 minutes (UTC)`);
}

async function startServer() {
  try {
    await checkDbConnection();
    await initTable();

    app.use("/", routes);
    scheduleSyncAndUpdate();

    const port = process.env.PORT || 3000;
    app.listen(port, () => {
      console.log(`Backend running on http://localhost:${port}`);
    });
  } catch (err: any) {
    console.error(`Startup failed: ${err.message}`);
    process.exit(1);
  }
}

startServer();
