# Backend Setup

This is the backend service for your project. It connects to multiple PostgreSQL databases and various blockchain RPC endpoints. Follow the steps below to get started.

## 🚀 Setup Instructions

1. Clone the repository
2. Create a `.env` file in the root directory based on the `.env.example` below
3. Run `npm install` to install dependencies
4. Start the server: `npm run start`

---

## 📄 .env.example

```dotenv
# Main PostgreSQL DB
MAIN_DB_NAME=
MAIN_DB_USER=
MAIN_DB_PASSWORD=
MAIN_DB_HOST=
MAIN_DB_PORT=

# Analysis PostgreSQL DB
ANALYSIS_DB_NAME=
ANALYSIS_DB_USER=
ANALYSIS_DB_PASSWORD=
ANALYSIS_DB_HOST=
ANALYSIS_DB_PORT=

# Server port
PORT=

# Alchemy Token (for authenticated RPC access)
ALCHEMY_TOKEN=

# RPC URLs
RPC_URL_CITREA_TESTNET=
RPC_URL_STARKNET_SEPOLIA=
RPC_URL_MONAD_TESTNET=
RPC_URL_HYPERLIQUID_TESTNET=
RPC_URL_BITCOIN_TESTNET=
