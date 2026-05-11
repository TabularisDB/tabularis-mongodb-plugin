<div align="center">
  <img src="https://raw.githubusercontent.com/debba/tabularis/main/public/logo-sm.png" width="120" height="120" />
</div>

# tabularis-mongodb-plugin
<p align="center">

![](https://img.shields.io/github/release/tabularisDB/tabularis-mongodb-plugin.svg?style=flat)
![](https://img.shields.io/github/downloads/tabularisDB/tabularis-mongodb-plugin/total.svg?style=flat)
![Build & Release](https://github.com/tabularisDB/tabularis-mongodb-plugin/workflows/Release/badge.svg)
[![Discord](https://img.shields.io/discord/1502944695808950282?color=5865F2&logo=discord&logoColor=white)](https://discord.com/invite/K2hmhfHRSt)

</p>

A [MongoDB](https://www.mongodb.com/) plugin for [Tabularis](https://github.com/debba/tabularis), the lightweight database management tool.

This plugin enables Tabularis to connect to any MongoDB instance and provides collection browsing, document-level CRUD, index management, and aggregation pipeline execution through a JSON-RPC 2.0 over stdio interface.

**Discord** - [Join our discord server](https://discord.com/invite/K2hmhfHRSt) and chat with the maintainers.

## Table of Contents

- [Features](#features)
- [Supported MongoDB Data Types](#supported-mongodb-data-types)
- [Installation](#installation)
  - [Automatic (via Tabularis)](#automatic-via-tabularis)
  - [Manual Installation](#manual-installation)
- [How It Works](#how-it-works)
- [Query Syntax](#query-syntax)
- [Supported Operations](#supported-operations)
- [Building from Source](#building-from-source)
- [Development](#development)
- [Changelog](#changelog)
- [License](#license)

## Features

- **Connection** — Connect to any MongoDB instance via host/port or full URI, with optional authentication.
- **Collection Browsing** — List databases, collections, and infer schema by sampling documents.
- **Schema Inference** — Automatically detects field names and BSON types by sampling up to 100 documents per collection.
- **Index Inspection** — List collection indexes with name, columns, uniqueness, and primary flag.
- **Query Execution** — Run `find`, `findOne`, `aggregate`, and `count` queries using MongoDB shell syntax.
- **Inline Editing** — Insert, update, and delete documents directly from the Tabularis data grid.
- **ObjectId Handling** — Automatically converts string primary key values to `ObjectId` when filtering by `_id`.
- **DDL-equivalent Generation** — Generates MongoDB shell commands for `createCollection`, `createIndex`, `$rename`, and more.
- **Cross-platform** — Pre-built binaries for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64).

## Supported MongoDB Data Types

| Category | Types |
|---|---|
| **Numeric** | Int32, Int64, Double, Decimal128 |
| **String** | String |
| **Date/Time** | Date |
| **Binary** | Binary |
| **Other** | Boolean, ObjectId, Array, Object |

## Installation

### Automatic (via Tabularis)

If your version of Tabularis supports plugin management, the MongoDB plugin can be installed directly from the application.

### Manual Installation

1. Download the latest release for your platform from the [Releases page](https://github.com/debba/tabularis-mongodb-plugin/releases).
2. Extract the archive.
3. Copy `tabularis-mongodb-plugin` (or `tabularis-mongodb-plugin.exe` on Windows) and `manifest.json` into the Tabularis plugins directory:

| OS | Plugins Directory |
|---|---|
| **Linux** | `~/.local/share/tabularis/plugins/mongodb/` |
| **macOS** | `~/Library/Application Support/com.debba.tabularis/plugins/mongodb/` |
| **Windows** | `%APPDATA%\com.debba.tabularis\plugins\mongodb\` |

4. Restart Tabularis.

## How It Works

The plugin is a standalone Rust binary that communicates with Tabularis through **JSON-RPC 2.0 over stdio**:

1. Tabularis spawns the plugin as a child process.
2. Requests are sent as newline-delimited JSON-RPC messages to the plugin's `stdin`.
3. The plugin connects to MongoDB using the official Rust driver and writes responses to `stdout`.

Connection state is pooled per URI — the same `Client` instance is reused for all operations within a session.

## Query Syntax

The `execute_query` method accepts two formats:

### MongoDB Shell Syntax (recommended)

```js
// Find documents
db.users.find({"age": {"$gt": 18}})

// Find with projection
db.users.find({"active": true}, {"name": 1, "email": 1})

// Find first match
db.orders.findOne({"status": "pending"})

// Aggregation pipeline
db.orders.aggregate([
  {"$match": {"status": "shipped"}},
  {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}}
])

// Count documents
db.users.countDocuments({"role": "admin"})
```

### SQL-like (for table browsing)

When Tabularis browses a collection it sends `SELECT * FROM collection_name`.
The plugin automatically converts this to `find({})` on the named collection.

## Supported Operations

| Method | Description |
|---|---|
| `test_connection` | Ping the MongoDB server |
| `get_databases` | List all databases |
| `get_schemas` | Returns `[]` (MongoDB has no schemas) |
| `get_tables` | List collections in the current database |
| `get_columns` | Infer fields and types by sampling documents |
| `get_foreign_keys` | Returns `[]` (no FK constraints in MongoDB) |
| `get_indexes` | List indexes for a collection |
| `execute_query` | Run shell-syntax queries with pagination |
| `insert_record` | Insert a new document |
| `update_record` | Update a single field by `_id` (or other PK) |
| `delete_record` | Delete a document by `_id` (or other PK) |
| `get_schema_snapshot` | Full schema dump (all collections + inferred columns) |
| `get_all_columns_batch` | Inferred columns for all collections |
| `get_all_foreign_keys_batch` | Returns `{}` |
| `get_create_table_sql` | Generates `db.createCollection(...)` |
| `get_add_column_sql` | Returns a schemaless note |
| `get_alter_column_sql` | Generates `$rename` update command |
| `get_create_index_sql` | Generates `db.collection.createIndex(...)` |
| `get_create_foreign_key_sql` | Returns a not-supported note |
| `drop_index` | Drops a collection index |

## Building from Source

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (edition 2021)
- A running MongoDB instance (for integration tests)

### Build

```bash
cargo build --release
```

The binary will be located at `target/release/tabularis-mongodb-plugin`.

### Install Locally

A convenience script is provided to build and copy the plugin to the Tabularis plugins directory:

```bash
./sync.sh
```

## Development

### Testing the Plugin

Two development binaries are included:

**Connectivity test** (requires a local MongoDB on port 27017):

```bash
cargo run --bin test
# or with a custom URI:
MONGODB_URI="mongodb://user:pass@host:27017/mydb" cargo run --bin test
```

**Simulated Tabularis integration test** (spawns the plugin and sends JSON-RPC requests via stdio):

```bash
cargo run --bin test_plugin
```

### Manual JSON-RPC test via shell

```bash
echo '{"jsonrpc":"2.0","method":"test_connection","params":{"params":{"host":"localhost","port":27017,"database":"test"}},"id":1}' \
  | ./target/release/tabularis-mongodb-plugin
```

### Tech Stack

- **Language:** Rust (edition 2021)
- **Database driver:** [mongodb](https://crates.io/crates/mongodb) v3 (official async Rust driver)
- **Serialization:** serde + serde_json + bson
- **Async runtime:** tokio
- **Protocol:** JSON-RPC 2.0 over stdio

## [Changelog](./CHANGELOG.md)

## License

Apache License 2.0
