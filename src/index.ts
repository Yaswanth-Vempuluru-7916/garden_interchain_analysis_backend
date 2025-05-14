import express, { Express } from "express";
import cors from "cors";
import dotenv from "dotenv";
import { analysisPool } from "./config/db";
import { initTable } from "./services/dbService";
import routes from "./routes";
import cron from "node-cron";
import { syncOrders, updateTimestamps } from "./controllers/syncController";

dotenv.config();

const app: Express = express();
app.use(cors());
app.use(express.json());

async function testDbConnection() {
  let client;
  try {
    client = await analysisPool.connect();
    console.log("Database connection successful");
  } catch (err: any) {
    console.error(`Database connection failed: ${err.message}\nStack: ${err.stack}`);
    process.exit(1);
  } finally {
    if (client) client.release();
  }
}

let isRunning = false;

const scheduleSyncAndUpdate = () => {
  cron.schedule("0 */2 * * *", async () => {
    if (isRunning) {
      console.log("Previous job still running, skipping this run...");
      return;
    }
    isRunning = true;
    console.log("Running scheduled sync and timestamp update...");
    try {
      await syncOrders(
        { body: {} } as any,
        {
          status: () => ({ json: () => {} }),
          json: () => {},
        } as any
      );
      console.log("Sync completed, proceeding to update timestamps...");
      await updateTimestamps(
        { body: {} } as any,
        {
          status: () => ({ json: () => {} }),
          json: () => {},
        } as any
      );
      console.log("Timestamp update completed.");
    } catch (err: any) {
      console.error("Error during scheduled sync/update:", err.message);
    } finally {
      isRunning = false;
    }
  });
  console.log("Cron job scheduled to run sync and updateTimestamps every 2 hours.");
};

const port = process.env.PORT || 3000;
testDbConnection()
  .then(() => {
    initTable()
      .then(() => {
        app.use("/", routes);
        //Start cron job after server initialization
        // scheduleSyncAndUpdate();
        app.listen(port, () => {
          console.log(`Backend running on http://localhost:${port}`);
        });
      })
      .catch((err) => {
        console.error("Failed to initialize table:", err.message);
        process.exit(1);
      });
  })
  .catch((err) => {
    console.error("Failed to connect to database:", err.message);
    process.exit(1);
  });