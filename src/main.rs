use bson::{doc, Bson, Document};
use futures::StreamExt;
use mongodb::{options::FindOptions, Client};
use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut clients: HashMap<String, Client> = HashMap::new();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Error reading from stdin: {}", e);
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let req: JsonValue = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to parse request: {}", e);
                continue;
            }
        };

        let id = req["id"].clone();
        let method = match req["method"].as_str() {
            Some(m) => m.to_string(),
            None => {
                send_error(&mut stdout, id, -32600, "Method not specified");
                continue;
            }
        };

        let params = &req["params"];
        let conn_params = &params["params"];

        let uri = match build_uri(conn_params) {
            Ok(uri) => uri,
            Err(error) => {
                send_error(&mut stdout, id, -32602, &error);
                continue;
            }
        };
        let explicit_database = match get_database_param(conn_params) {
            Ok(database) => database,
            Err(error) => {
                send_error(&mut stdout, id, -32602, &error);
                continue;
            }
        };
        // Multi-database navigation: the host sends the database the user
        // selected in the request-level `schema` field, which outranks the
        // database stored on the connection.
        let request_database = match get_string_param(params, "schema") {
            Ok(database) => database.map(str::to_owned),
            Err(error) => {
                send_error(&mut stdout, id, -32602, &error);
                continue;
            }
        };

        let client = match get_or_create_client(&rt, &mut clients, &uri) {
            Ok(c) => c,
            Err(e) => {
                send_error(
                    &mut stdout,
                    id,
                    -32000,
                    &format!("Failed to connect to MongoDB: {}", e),
                );
                continue;
            }
        };
        let default_database = client.default_database();
        let db_name = resolve_database_name(
            request_database.as_deref().or(explicit_database.as_deref()),
            default_database.as_ref().map(|database| database.name()),
        );

        match method.as_str() {
            "test_connection" => match rt.block_on(test_connection(client, &db_name)) {
                Ok(v) => send_success(&mut stdout, id, v),
                Err(e) => send_error(&mut stdout, id, -32000, &e),
            },
            "get_databases" => {
                let result = rt.block_on(get_databases(client, &db_name));
                send_success(&mut stdout, id, result);
            }
            "get_schemas" => {
                send_success(&mut stdout, id, json!([]));
            }
            "get_tables" => match rt.block_on(get_collections(client, &db_name)) {
                Ok(v) => send_success(&mut stdout, id, v),
                Err(e) => send_error(&mut stdout, id, -32001, &e),
            },
            "get_columns" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                match rt.block_on(get_columns(client, &db_name, table)) {
                    Ok(v) => send_success(&mut stdout, id, v),
                    Err(e) => send_error(&mut stdout, id, -32002, &e),
                }
            }
            "get_foreign_keys" => {
                send_success(&mut stdout, id, json!([]));
            }
            "get_indexes" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                match rt.block_on(get_indexes(client, &db_name, table)) {
                    Ok(v) => send_success(&mut stdout, id, v),
                    Err(e) => send_error(&mut stdout, id, -32004, &e),
                }
            }
            "get_views" | "get_view_definition" | "get_view_columns" => {
                send_success(&mut stdout, id, json!([]));
            }
            "create_view" | "alter_view" | "drop_view" => {
                send_error(
                    &mut stdout,
                    id,
                    -32601,
                    "Views are not supported in this driver",
                );
            }
            "get_routines" | "get_routine_parameters" => {
                send_success(&mut stdout, id, json!([]));
            }
            "get_routine_definition" => {
                send_error(
                    &mut stdout,
                    id,
                    -32601,
                    "MongoDB does not support stored routines",
                );
            }
            "execute_query" => {
                let query = params.get("query").and_then(|q| q.as_str()).unwrap_or("");
                let limit = params
                    .get("limit")
                    .and_then(|l| l.as_u64())
                    .map(|l| l as u32);
                let page = params
                    .get("page")
                    .and_then(|p| p.as_u64())
                    .map(|p| p as u32)
                    .unwrap_or(1);
                match rt.block_on(execute_query(client, &db_name, query, limit, page)) {
                    Ok(v) => send_success(&mut stdout, id, v),
                    Err(e) => send_error(&mut stdout, id, -32012, &e),
                }
            }
            "insert_record" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let data = params
                    .get("data")
                    .and_then(|d| d.as_object())
                    .cloned()
                    .unwrap_or_default();
                match rt.block_on(insert_record(client, &db_name, table, &data)) {
                    Ok(n) => send_success(&mut stdout, id, json!(n)),
                    Err(e) => send_error(&mut stdout, id, -32013, &e),
                }
            }
            "update_record" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let pk_col = params
                    .get("pk_col")
                    .and_then(|p| p.as_str())
                    .unwrap_or("_id");
                let pk_val = params.get("pk_val").cloned().unwrap_or(JsonValue::Null);
                let col_name = params
                    .get("col_name")
                    .and_then(|c| c.as_str())
                    .unwrap_or("");
                let new_val = params.get("new_val").cloned().unwrap_or(JsonValue::Null);
                match rt.block_on(update_record(
                    client, &db_name, table, pk_col, &pk_val, col_name, &new_val,
                )) {
                    Ok(n) => send_success(&mut stdout, id, json!(n)),
                    Err(e) => send_error(&mut stdout, id, -32014, &e),
                }
            }
            "delete_record" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let pk_col = params
                    .get("pk_col")
                    .and_then(|p| p.as_str())
                    .unwrap_or("_id");
                let pk_val = params.get("pk_val").cloned().unwrap_or(JsonValue::Null);
                match rt.block_on(delete_record(client, &db_name, table, pk_col, &pk_val)) {
                    Ok(n) => send_success(&mut stdout, id, json!(n)),
                    Err(e) => send_error(&mut stdout, id, -32015, &e),
                }
            }
            "get_schema_snapshot" => match rt.block_on(get_schema_snapshot(client, &db_name)) {
                Ok(v) => send_success(&mut stdout, id, v),
                Err(e) => send_error(&mut stdout, id, -32016, &e),
            },
            "get_all_columns_batch" => match rt.block_on(get_all_columns_batch(client, &db_name)) {
                Ok(v) => send_success(&mut stdout, id, v),
                Err(e) => send_error(&mut stdout, id, -32017, &e),
            },
            "get_all_foreign_keys_batch" => {
                send_success(&mut stdout, id, json!({}));
            }
            "get_create_table_sql" => {
                let table_name = params
                    .get("table_name")
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                send_success(
                    &mut stdout,
                    id,
                    json!([format!("db.createCollection(\"{}\")", table_name)]),
                );
            }
            "get_add_column_sql" => {
                send_success(
                    &mut stdout,
                    id,
                    json!(["// MongoDB is schemaless — fields are added automatically on insert"]),
                );
            }
            "get_alter_column_sql" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let old_name = params
                    .get("old_column")
                    .and_then(|c| c.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let new_name = params
                    .get("new_column")
                    .and_then(|c| c.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                if old_name != new_name && !old_name.is_empty() {
                    send_success(
                        &mut stdout,
                        id,
                        json!([format!(
                            "db.{}.updateMany({{}}, {{\"$rename\": {{\"{}\" : \"{}\"}}}})",
                            table, old_name, new_name
                        )]),
                    );
                } else {
                    send_success(&mut stdout, id, json!(["// No rename needed in MongoDB"]));
                }
            }
            "get_create_index_sql" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let columns: Vec<String> = params
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let is_unique = params
                    .get("is_unique")
                    .and_then(|u| u.as_bool())
                    .unwrap_or(false);
                let key_doc: String = columns
                    .iter()
                    .map(|c| format!("\"{}\": 1", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let unique_opt = if is_unique { ", { unique: true }" } else { "" };
                send_success(
                    &mut stdout,
                    id,
                    json!([format!(
                        "db.{}.createIndex({{ {} }}{})",
                        table, key_doc, unique_opt
                    )]),
                );
            }
            "get_create_foreign_key_sql" => {
                send_success(
                    &mut stdout,
                    id,
                    json!(["// MongoDB does not support foreign key constraints"]),
                );
            }
            "drop_index" => {
                let table = params.get("table").and_then(|t| t.as_str()).unwrap_or("");
                let index_name = params
                    .get("index_name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                match rt.block_on(drop_index(client, &db_name, table, index_name)) {
                    Ok(_) => send_success(&mut stdout, id, json!(null)),
                    Err(e) => send_error(&mut stdout, id, -32024, &e),
                }
            }
            "drop_foreign_key" => {
                send_error(
                    &mut stdout,
                    id,
                    -32601,
                    "MongoDB does not support foreign key constraints",
                );
            }
            _ => {
                send_error(
                    &mut stdout,
                    id,
                    -32601,
                    &format!("Method '{}' not implemented", method),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection management
// ---------------------------------------------------------------------------

fn build_uri(params: &JsonValue) -> Result<String, String> {
    const URI_KEYS: [&str; 7] = [
        "connection_string",
        "connectionString",
        "connection_uri",
        "connectionUri",
        "uri",
        "url",
        "dsn",
    ];

    for key in URI_KEYS {
        if let Some(uri) = get_string_param(params, key)? {
            validate_mongo_scheme(uri)?;
            return Ok(uri.to_string());
        }
    }

    let host = get_string_param(params, "host")?.unwrap_or("localhost");
    let port = params.get("port").and_then(|p| p.as_u64()).unwrap_or(27017);
    let database = get_database_param(params)?.unwrap_or_else(|| "admin".to_string());
    let username = get_string_param(params, "username")?.unwrap_or("");
    let password = get_string_param(params, "password")?.unwrap_or("");

    let mut uri = if !username.is_empty() {
        format!(
            "mongodb://{}:{}@{}:{}/{}",
            encode_uri_component(username),
            encode_uri_component(password),
            host,
            port,
            database
        )
    } else {
        format!("mongodb://{}:{}/{}", host, port, database)
    };

    append_query_options(&mut uri, params)?;
    Ok(uri)
}

/// The host stores `database` as a string for single-database drivers and as
/// an array of selected names for multi-database ones. Accept both; for an
/// array the first entry is the initial database, and an empty array means
/// none was chosen yet.
fn get_database_param(params: &JsonValue) -> Result<Option<String>, String> {
    match params.get("database") {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) if value.trim().is_empty() => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Array(values)) => match values.first() {
            None => Ok(None),
            Some(JsonValue::String(value)) if value.trim().is_empty() => Ok(None),
            Some(JsonValue::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err("Parameter 'database' must contain strings".to_string()),
        },
        Some(_) => Err("Parameter 'database' must be a string or an array of strings".to_string()),
    }
}

fn get_string_param<'a>(params: &'a JsonValue, key: &str) -> Result<Option<&'a str>, String> {
    match params.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) if value.trim().is_empty() => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value)),
        Some(_) => Err(format!("Parameter '{key}' must be a string")),
    }
}

fn resolve_database_name(explicit_database: Option<&str>, uri_database: Option<&str>) -> String {
    explicit_database
        .or(uri_database.filter(|database| !database.is_empty()))
        .unwrap_or("admin")
        .to_string()
}

fn validate_mongo_scheme(uri: &str) -> Result<(), String> {
    if uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://") {
        Ok(())
    } else {
        Err("Unsupported MongoDB URI scheme; expected mongodb:// or mongodb+srv://".to_string())
    }
}

fn get_bool_param(params: &JsonValue, key: &str) -> Result<Option<bool>, String> {
    match params.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(JsonValue::String(value)) if value.trim().is_empty() => Ok(None),
        Some(JsonValue::String(value)) if value.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(JsonValue::String(value)) if value.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(_) => Err(format!("Parameter '{key}' must be a boolean")),
    }
}

fn append_query_options(uri: &mut String, params: &JsonValue) -> Result<(), String> {
    let mut options = Vec::new();
    let mut names = HashSet::new();

    for key in ["tls", "ssl"] {
        if let Some(value) = get_bool_param(params, key)? {
            push_query_option(&mut options, &mut names, key, value.to_string())?;
        }
    }

    for key in ["authSource", "replicaSet"] {
        if let Some(value) = get_string_param(params, key)? {
            push_query_option(&mut options, &mut names, key, value.to_string())?;
        }
    }

    if let Some(value) = get_bool_param(params, "retryWrites")? {
        push_query_option(&mut options, &mut names, "retryWrites", value.to_string())?;
    }

    if let Some(value) = get_write_concern(params)? {
        push_query_option(&mut options, &mut names, "w", value)?;
    }

    if let Some(value) = get_string_param(params, "appName")? {
        push_query_option(&mut options, &mut names, "appName", value.to_string())?;
    }

    if let Some(value) = get_bool_param(params, "directConnection")? {
        push_query_option(
            &mut options,
            &mut names,
            "directConnection",
            value.to_string(),
        )?;
    }

    append_extra_options(params, &mut options, &mut names)?;

    if !options.is_empty() {
        uri.push('?');
        uri.push_str(
            &options
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        encode_uri_component(&key),
                        encode_uri_component(&value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }

    Ok(())
}

fn get_write_concern(params: &JsonValue) -> Result<Option<String>, String> {
    match params.get("w") {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) if value.trim().is_empty() => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Number(value)) => Ok(Some(value.to_string())),
        Some(_) => Err("Parameter 'w' must be a string or number".to_string()),
    }
}

fn append_extra_options(
    params: &JsonValue,
    options: &mut Vec<(String, String)>,
    names: &mut HashSet<String>,
) -> Result<(), String> {
    let Some(extra_options) = params.get("extra_options") else {
        return Ok(());
    };

    match extra_options {
        JsonValue::Null => Ok(()),
        JsonValue::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));

            for (key, value) in entries {
                let value = query_scalar_to_string(value, key)?;
                push_query_option(options, names, key, value)?;
            }
            Ok(())
        }
        JsonValue::String(value) => append_extra_options_string(value, options, names),
        _ => Err("Parameter 'extra_options' must be an object or query string".to_string()),
    }
}

fn append_extra_options_string(
    value: &str,
    options: &mut Vec<(String, String)>,
    names: &mut HashSet<String>,
) -> Result<(), String> {
    let query = value.trim().strip_prefix('?').unwrap_or(value.trim());
    if query.is_empty() {
        return Ok(());
    }

    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err("Parameter 'extra_options' must contain key=value query pairs".to_string());
        };
        if key.is_empty()
            || value.is_empty()
            || key.chars().any(char::is_whitespace)
            || value.chars().any(char::is_whitespace)
            || key.contains(['?', '#'])
            || value.contains(['?', '#'])
        {
            return Err("Parameter 'extra_options' contains an invalid query pair".to_string());
        }
        push_query_option(options, names, key, value.to_string())?;
    }

    Ok(())
}

fn query_scalar_to_string(value: &JsonValue, key: &str) -> Result<String, String> {
    match value {
        JsonValue::String(value) if !value.is_empty() => Ok(value.clone()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Number(value) => Ok(value.to_string()),
        _ => Err(format!(
            "Extra option '{key}' must have a non-empty scalar value"
        )),
    }
}

fn push_query_option(
    options: &mut Vec<(String, String)>,
    names: &mut HashSet<String>,
    key: &str,
    value: String,
) -> Result<(), String> {
    if key.is_empty() {
        return Err("MongoDB query option names cannot be empty".to_string());
    }
    if !names.insert(key.to_string()) {
        return Err(format!(
            "MongoDB query option '{key}' was provided more than once"
        ));
    }
    options.push((key.to_string(), value));
    Ok(())
}

fn encode_uri_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }

    encoded
}

fn mask_mongo_uri(value: &str) -> String {
    const SCHEMES: [&str; 2] = ["mongodb+srv://", "mongodb://"];
    let mut masked = String::with_capacity(value.len());
    let mut cursor = 0;

    loop {
        let next_scheme = SCHEMES
            .iter()
            .filter_map(|scheme| value[cursor..].find(scheme).map(|index| (index, *scheme)))
            .min_by_key(|(index, _)| *index);

        let Some((scheme_index, scheme)) = next_scheme else {
            masked.push_str(&value[cursor..]);
            break;
        };

        let uri_start = cursor + scheme_index;
        let uri_body_start = uri_start + scheme.len();
        masked.push_str(&value[cursor..uri_start]);
        masked.push_str(scheme);
        masked.push_str("<redacted>");

        let opening_quote = value[..uri_start]
            .chars()
            .next_back()
            .filter(|character| matches!(character, '"' | '\''));
        let token_end = match opening_quote {
            Some(quote) => value[uri_body_start..]
                .find(quote)
                .map(|index| uri_body_start + index)
                .unwrap_or(value.len()),
            None => value[uri_body_start..]
                .find(|character: char| {
                    character.is_whitespace() || matches!(character, '"' | '\'')
                })
                .map(|index| uri_body_start + index)
                .unwrap_or(value.len()),
        };
        cursor = token_end;
    }

    masked
}

fn safe_mongo_error(error: impl std::fmt::Display) -> String {
    mask_mongo_uri(&error.to_string())
}

fn get_or_create_client<'a>(
    rt: &tokio::runtime::Runtime,
    clients: &'a mut HashMap<String, Client>,
    uri: &str,
) -> Result<&'a Client, String> {
    if !clients.contains_key(uri) {
        let client = rt.block_on(Client::with_uri_str(uri)).map_err(|error| {
            format!(
                "Failed to create MongoDB client: {}",
                safe_mongo_error(error)
            )
        })?;
        clients.insert(uri.to_string(), client);
    }
    Ok(clients.get(uri).unwrap())
}

// ---------------------------------------------------------------------------
// JSON-RPC helpers
// ---------------------------------------------------------------------------

fn send_success(stdout: &mut io::Stdout, id: JsonValue, result: JsonValue) {
    let response = json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id
    });
    let mut res_str = serde_json::to_string(&response).unwrap();
    res_str.push('\n');
    stdout.write_all(res_str.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

fn send_error(stdout: &mut io::Stdout, id: JsonValue, code: i32, message: &str) {
    let response = json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message
        },
        "id": id
    });
    let mut res_str = serde_json::to_string(&response).unwrap();
    res_str.push('\n');
    stdout.write_all(res_str.as_bytes()).unwrap();
    stdout.flush().unwrap();
}

// ---------------------------------------------------------------------------
// Schema discovery
// ---------------------------------------------------------------------------

async fn test_connection(client: &Client, db_name: &str) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    db.run_command(doc! { "ping": 1 })
        .await
        .map(|_| json!({ "success": true }))
        .map_err(safe_mongo_error)
}

async fn get_databases(client: &Client, db_name: &str) -> JsonValue {
    match client.list_database_names().await {
        Ok(names) => json!(names),
        Err(_) => json!([db_name]),
    }
}

async fn get_collections(client: &Client, db_name: &str) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    let names = db.list_collection_names().await.map_err(safe_mongo_error)?;
    let result: Vec<JsonValue> = names
        .iter()
        .map(|name| json!({ "name": name, "schema": db_name, "comment": null }))
        .collect();
    Ok(json!(result))
}

/// Infers the schema of a collection by sampling up to 100 documents.
/// Returns columns with `_id` first, then other fields sorted alphabetically.
async fn get_columns(
    client: &Client,
    db_name: &str,
    collection_name: &str,
) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let options = FindOptions::builder().limit(100i64).build();
    let mut cursor = collection
        .find(doc! {})
        .with_options(options)
        .await
        .map_err(safe_mongo_error)?;

    // field name → list of observed BSON type names
    let mut field_types: HashMap<String, Vec<&'static str>> = HashMap::new();
    // field name → number of documents containing it
    let mut field_seen: HashMap<String, usize> = HashMap::new();
    let mut total_docs = 0usize;

    while let Some(result) = cursor.next().await {
        let doc: Document = result.map_err(safe_mongo_error)?;
        total_docs += 1;
        for (key, value) in &doc {
            *field_seen.entry(key.clone()).or_insert(0) += 1;
            field_types
                .entry(key.clone())
                .or_default()
                .push(bson_type_name(value));
        }
    }

    let mut columns: Vec<JsonValue> = Vec::new();

    // _id is always primary key
    let id_type = field_types
        .get("_id")
        .and_then(|types| types.first().copied())
        .unwrap_or("ObjectId");
    columns.push(json!({
        "name": "_id",
        "data_type": id_type,
        "is_pk": true,
        "is_nullable": false,
        "is_auto_increment": true,
        "default_value": null,
    }));

    // Collect remaining fields, sorted alphabetically
    let mut other_fields: Vec<(String, &'static str, bool)> = field_types
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .map(|(name, types)| {
            // Pick the most frequent type
            let mut counts: HashMap<&str, usize> = HashMap::new();
            for t in types {
                *counts.entry(t).or_insert(0) += 1;
            }
            let dominant = counts
                .into_iter()
                .max_by_key(|(_, c)| *c)
                .map(|(t, _)| t)
                .unwrap_or("Mixed");
            let seen_count = *field_seen.get(name.as_str()).unwrap_or(&0);
            let is_nullable = seen_count < total_docs;
            (name.clone(), dominant, is_nullable)
        })
        .collect();

    other_fields.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, type_name, is_nullable) in other_fields {
        columns.push(json!({
            "name": name,
            "data_type": type_name,
            "is_pk": false,
            "is_nullable": is_nullable,
            "is_auto_increment": false,
            "default_value": null,
        }));
    }

    Ok(json!(columns))
}

/// Lists indexes for a collection using the raw `listIndexes` command.
async fn get_indexes(
    client: &Client,
    db_name: &str,
    collection_name: &str,
) -> Result<JsonValue, String> {
    let db = client.database(db_name);

    let cmd_result = db
        .run_command(doc! { "listIndexes": collection_name })
        .await
        .map_err(safe_mongo_error)?;

    let cursor_doc = cmd_result
        .get_document("cursor")
        .map_err(|_| "No cursor in listIndexes response".to_string())?;
    let first_batch = cursor_doc
        .get_array("firstBatch")
        .map_err(|_| "No firstBatch in listIndexes response".to_string())?;

    let mut indexes: Vec<JsonValue> = Vec::new();
    for item in first_batch {
        if let Bson::Document(idx_doc) = item {
            let name = idx_doc
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let key_doc = idx_doc.get_document("key").ok();
            let is_unique = idx_doc
                .get("unique")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let is_primary = name == "_id_";
            let columns: Vec<String> = key_doc
                .map(|kd| kd.keys().cloned().collect())
                .unwrap_or_default();

            indexes.push(json!({
                "name": name,
                "columns": columns,
                "is_unique": is_unique,
                "is_primary": is_primary,
            }));
        }
    }
    Ok(json!(indexes))
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Holds a parsed `db.collection.operation(args)` query.
struct ParsedShellQuery {
    collection: String,
    operation: String,
    args: Vec<String>,
}

fn parse_shell_query(query: &str) -> Option<ParsedShellQuery> {
    let q = query.trim().trim_end_matches(';');

    let rest = q.strip_prefix("db.")?;
    let dot = rest.find('.')?;
    let collection = rest[..dot].trim().to_string();
    let after_coll = &rest[dot + 1..];

    let paren = after_coll.find('(')?;
    let operation = after_coll[..paren].trim().to_string();
    let after_paren = &after_coll[paren + 1..];

    let args_str = find_balanced_args(after_paren)?;
    let args = split_top_level_args(args_str);

    Some(ParsedShellQuery {
        collection,
        operation,
        args,
    })
}

/// Returns the substring before the matching `)` at depth 0.
fn find_balanced_args(s: &str) -> Option<&str> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;

    for (i, ch) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && in_string {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' | '[' | '(' => depth += 1,
            '}' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ')' => {
                if depth == 0 {
                    return Some(&s[..i]);
                }
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ => {}
        }
    }
    Some(s.trim())
}

/// Splits a string at top-level commas (not inside brackets/braces).
fn split_top_level_args(s: &str) -> Vec<String> {
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut args: Vec<String> = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for (i, ch) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && in_string {
            escaped = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' | '[' | '(' => depth += 1,
            '}' | ']' | ')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            ',' if depth == 0 => {
                args.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim().to_string();
    if !last.is_empty() {
        args.push(last);
    }
    args
}

/// Parses a JSON object string into a BSON Document.
fn parse_filter_str(s: &str) -> Result<Document, String> {
    let s = s.trim();
    if s.is_empty() || s == "{}" {
        return Ok(doc! {});
    }
    let json_val: JsonValue =
        serde_json::from_str(s).map_err(|e| format!("Invalid filter JSON: {}", e))?;
    match json_to_bson(&json_val) {
        Bson::Document(doc) => Ok(doc),
        _ => Err("Filter must be a JSON object".to_string()),
    }
}

/// Parses a JSON array string into a BSON aggregation pipeline.
fn parse_pipeline_str(s: &str) -> Result<Vec<Document>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(vec![]);
    }
    let json_val: JsonValue =
        serde_json::from_str(s).map_err(|e| format!("Invalid pipeline JSON: {}", e))?;
    match json_val {
        JsonValue::Array(arr) => arr
            .iter()
            .map(|item| match json_to_bson(item) {
                Bson::Document(doc) => Ok(doc),
                _ => Err("Pipeline stage must be a JSON object".to_string()),
            })
            .collect(),
        _ => Err("Pipeline must be a JSON array".to_string()),
    }
}

fn parse_arg_as_doc(arg: Option<&String>) -> Result<Document, String> {
    match arg {
        None => Ok(doc! {}),
        Some(s) => {
            let s = s.trim();
            if s.is_empty() {
                Ok(doc! {})
            } else {
                parse_filter_str(s)
            }
        }
    }
}

fn parse_arg_as_pipeline(arg: Option<&String>) -> Result<Vec<Document>, String> {
    match arg {
        None => Ok(vec![]),
        Some(s) => {
            let s = s.trim();
            if s.is_empty() {
                Ok(vec![])
            } else {
                parse_pipeline_str(s)
            }
        }
    }
}

/// Tries to extract a collection name from a SQL-like `SELECT ... FROM collection` query.
fn parse_sql_from_clause(query: &str) -> Option<String> {
    let upper = query.trim().to_uppercase();
    let from_pos = upper.find(" FROM ")?;
    let rest = query[from_pos + 6..].trim();
    let name: &str = rest.split_whitespace().next()?;
    // The host quotes identifiers (`SELECT * FROM "plans"`); the quotes are
    // not part of the collection name.
    let name = name.trim_matches(|c| c == '"' || c == '`' || c == '\'');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

async fn execute_query(
    client: &Client,
    db_name: &str,
    query: &str,
    limit: Option<u32>,
    page: u32,
) -> Result<JsonValue, String> {
    let q = query.trim();

    if let Some(parsed) = parse_shell_query(q) {
        return dispatch_shell_query(client, db_name, parsed, limit, page).await;
    }

    if let Some(collection_name) = parse_sql_from_clause(q) {
        return execute_find(
            client,
            db_name,
            &collection_name,
            doc! {},
            None,
            limit,
            page,
        )
        .await;
    }

    Err(
        "Invalid query format. Use MongoDB shell syntax:\n  db.collection.find({})\n  db.collection.aggregate([...])"
            .to_string(),
    )
}

async fn dispatch_shell_query(
    client: &Client,
    db_name: &str,
    parsed: ParsedShellQuery,
    limit: Option<u32>,
    page: u32,
) -> Result<JsonValue, String> {
    let coll = &parsed.collection;
    let args = &parsed.args;

    match parsed.operation.as_str() {
        "find" => {
            let filter = parse_arg_as_doc(args.first())?;
            let projection = if args.len() > 1 && !args[1].trim().is_empty() {
                Some(parse_arg_as_doc(args.get(1))?)
            } else {
                None
            };
            execute_find(client, db_name, coll, filter, projection, limit, page).await
        }
        "findOne" => {
            let filter = parse_arg_as_doc(args.first())?;
            execute_find(client, db_name, coll, filter, None, Some(1), 1).await
        }
        "aggregate" => {
            let pipeline = parse_arg_as_pipeline(args.first())?;
            execute_aggregate(client, db_name, coll, pipeline, limit, page).await
        }
        "count" | "countDocuments" => {
            let filter = parse_arg_as_doc(args.first())?;
            let db = client.database(db_name);
            let collection: mongodb::Collection<Document> = db.collection(coll);
            let count = collection
                .count_documents(filter)
                .await
                .map_err(safe_mongo_error)?;
            Ok(json!({
                "columns": ["count"],
                "rows": [[count]],
                "affected_rows": 0,
                "truncated": false,
                "has_more": false,
                "pagination": null,
            }))
        }
        "estimatedDocumentCount" => {
            let db = client.database(db_name);
            let collection: mongodb::Collection<Document> = db.collection(coll);
            let count = collection
                .estimated_document_count()
                .await
                .map_err(safe_mongo_error)?;
            Ok(json!({
                "columns": ["count"],
                "rows": [[count]],
                "affected_rows": 0,
                "truncated": false,
                "has_more": false,
                "pagination": null,
            }))
        }
        op => Err(format!(
            "Unsupported operation '{}'. Supported: find, findOne, aggregate, count, countDocuments",
            op
        )),
    }
}

async fn execute_find(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    filter: Document,
    projection: Option<Document>,
    limit: Option<u32>,
    page: u32,
) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let page = if page == 0 { 1 } else { page };
    let fetch_limit = limit.map(|l| (l + 1) as i64);
    let skip = limit.map(|l| ((page - 1) * l) as u64).unwrap_or(0u64);

    let mut options = FindOptions::builder().skip(skip).build();
    if let Some(l) = fetch_limit {
        options.limit = Some(l);
    }
    if let Some(proj) = projection {
        options.projection = Some(proj);
    }

    let mut cursor = collection
        .find(filter)
        .with_options(options)
        .await
        .map_err(safe_mongo_error)?;

    let mut all_docs: Vec<Document> = Vec::new();
    while let Some(result) = cursor.next().await {
        let doc: Document = result.map_err(safe_mongo_error)?;
        all_docs.push(doc);
    }

    let has_more = limit.map(|l| all_docs.len() > l as usize).unwrap_or(false);
    if has_more {
        all_docs.truncate(limit.unwrap() as usize);
    }

    let columns = collect_columns(&all_docs);
    let rows = docs_to_rows(&all_docs, &columns);

    Ok(json!({
        "columns": columns,
        "rows": rows,
        "affected_rows": 0,
        "truncated": has_more,
        "has_more": has_more,
        "pagination": if limit.is_some() {
            json!({
                "page": page,
                "page_size": limit.unwrap(),
                "total_rows": null,
                "has_more": has_more,
            })
        } else {
            JsonValue::Null
        },
    }))
}

async fn execute_aggregate(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    pipeline: Vec<Document>,
    limit: Option<u32>,
    page: u32,
) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let mut cursor = collection
        .aggregate(pipeline)
        .await
        .map_err(safe_mongo_error)?;

    let mut all_docs: Vec<Document> = Vec::new();
    while let Some(result) = cursor.next().await {
        let doc: Document = result.map_err(safe_mongo_error)?;
        all_docs.push(doc);
    }

    let page = if page == 0 { 1 } else { page };
    let (start, end, has_more) = if let Some(l) = limit {
        let s = ((page - 1) * l) as usize;
        let e = (s + l as usize).min(all_docs.len());
        let more = e < all_docs.len();
        (s, e, more)
    } else {
        (0usize, all_docs.len(), false)
    };

    let page_docs = &all_docs[start.min(all_docs.len())..end.min(all_docs.len())];
    let columns = collect_columns(page_docs);
    let rows = docs_to_rows(page_docs, &columns);

    Ok(json!({
        "columns": columns,
        "rows": rows,
        "affected_rows": 0,
        "truncated": has_more,
        "has_more": has_more,
        "pagination": if limit.is_some() {
            json!({
                "page": page,
                "page_size": limit.unwrap(),
                "total_rows": all_docs.len() as u64,
                "has_more": has_more,
            })
        } else {
            JsonValue::Null
        },
    }))
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

async fn insert_record(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    data: &serde_json::Map<String, JsonValue>,
) -> Result<u64, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let doc: Document = data
        .iter()
        .filter(|(k, v)| {
            // Skip null _id so MongoDB auto-generates it
            !(k.as_str() == "_id" && v.is_null())
        })
        .map(|(k, v)| {
            let bval = if k == "_id" {
                parse_bson_id(v)
            } else {
                json_to_bson(v)
            };
            (k.clone(), bval)
        })
        .collect();

    collection.insert_one(doc).await.map_err(safe_mongo_error)?;
    Ok(1)
}

async fn update_record(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    pk_col: &str,
    pk_val: &JsonValue,
    col_name: &str,
    new_val: &JsonValue,
) -> Result<u64, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let filter_bson = if pk_col == "_id" {
        parse_bson_id(pk_val)
    } else {
        json_to_bson(pk_val)
    };

    let filter = doc! { pk_col: filter_bson };
    let update = doc! { "$set": { col_name: json_to_bson(new_val) } };

    let result = collection
        .update_one(filter, update)
        .await
        .map_err(safe_mongo_error)?;
    Ok(result.modified_count)
}

async fn delete_record(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    pk_col: &str,
    pk_val: &JsonValue,
) -> Result<u64, String> {
    let db = client.database(db_name);
    let collection: mongodb::Collection<Document> = db.collection(collection_name);

    let filter_bson = if pk_col == "_id" {
        parse_bson_id(pk_val)
    } else {
        json_to_bson(pk_val)
    };

    let filter = doc! { pk_col: filter_bson };
    let result = collection
        .delete_one(filter)
        .await
        .map_err(safe_mongo_error)?;
    Ok(result.deleted_count)
}

async fn drop_index(
    client: &Client,
    db_name: &str,
    collection_name: &str,
    index_name: &str,
) -> Result<(), String> {
    let db = client.database(db_name);
    db.run_command(doc! {
        "dropIndexes": collection_name,
        "index": index_name,
    })
    .await
    .map(|_| ())
    .map_err(safe_mongo_error)
}

// ---------------------------------------------------------------------------
// Batch / snapshot
// ---------------------------------------------------------------------------

async fn get_schema_snapshot(client: &Client, db_name: &str) -> Result<JsonValue, String> {
    let collections = get_collections(client, db_name).await?;
    let names: Vec<String> = collections
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|c| c["name"].as_str().map(String::from))
        .collect();

    let mut snapshots = Vec::new();
    for name in &names {
        let columns = get_columns(client, db_name, name)
            .await
            .unwrap_or(json!([]));
        snapshots.push(json!({
            "name": name,
            "schema": db_name,
            "comment": null,
            "columns": columns,
            "foreign_keys": [],
        }));
    }
    Ok(json!(snapshots))
}

async fn get_all_columns_batch(client: &Client, db_name: &str) -> Result<JsonValue, String> {
    let db = client.database(db_name);
    let names = db.list_collection_names().await.map_err(safe_mongo_error)?;

    let mut result = serde_json::Map::new();
    for name in names {
        let cols = get_columns(client, db_name, &name)
            .await
            .unwrap_or(json!([]));
        result.insert(name, cols);
    }
    Ok(JsonValue::Object(result))
}

// ---------------------------------------------------------------------------
// Value conversion helpers
// ---------------------------------------------------------------------------

/// Returns a canonical BSON type name for display purposes.
fn bson_type_name(bson: &Bson) -> &'static str {
    match bson {
        Bson::Double(_) => "Double",
        Bson::String(_) => "String",
        Bson::Array(_) => "Array",
        Bson::Document(_) => "Object",
        Bson::Boolean(_) => "Boolean",
        Bson::Null => "Null",
        Bson::RegularExpression(_) => "RegExp",
        Bson::JavaScriptCode(_) => "JavaScript",
        Bson::Int32(_) => "Int32",
        Bson::Int64(_) => "Int64",
        Bson::Timestamp(_) => "Timestamp",
        Bson::Binary(_) => "Binary",
        Bson::ObjectId(_) => "ObjectId",
        Bson::DateTime(_) => "Date",
        Bson::Decimal128(_) => "Decimal128",
        _ => "Unknown",
    }
}

/// Converts a BSON value to a JSON value for transport.
fn bson_to_json(bson: &Bson) -> JsonValue {
    match bson {
        Bson::Double(f) => json!(f),
        Bson::String(s) => json!(s),
        Bson::Array(arr) => JsonValue::Array(arr.iter().map(bson_to_json).collect()),
        Bson::Document(doc) => {
            let obj: serde_json::Map<String, JsonValue> = doc
                .iter()
                .map(|(k, v)| (k.clone(), bson_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        Bson::Boolean(b) => json!(b),
        Bson::Null => JsonValue::Null,
        Bson::RegularExpression(re) => json!(format!("/{}/{}", re.pattern, re.options)),
        Bson::JavaScriptCode(code) => json!(code),
        Bson::Int32(i) => json!(i),
        Bson::Int64(i) => json!(i),
        Bson::Timestamp(ts) => json!(ts.time),
        Bson::Binary(b) => json!(format!("Binary({} bytes)", b.bytes.len())),
        Bson::ObjectId(oid) => json!(oid.to_hex()),
        Bson::DateTime(dt) => json!(dt.timestamp_millis()),
        Bson::Symbol(s) => json!(s),
        Bson::Decimal128(d) => json!(d.to_string()),
        Bson::Undefined | Bson::MinKey | Bson::MaxKey => JsonValue::Null,
        _ => json!(bson.to_string()),
    }
}

/// Converts a JSON value to a BSON value.
fn json_to_bson(val: &JsonValue) -> Bson {
    match val {
        JsonValue::Null => Bson::Null,
        JsonValue::Bool(b) => Bson::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        JsonValue::String(s) => Bson::String(s.clone()),
        JsonValue::Array(arr) => Bson::Array(arr.iter().map(json_to_bson).collect()),
        JsonValue::Object(obj) => {
            let doc: Document = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bson(v)))
                .collect();
            Bson::Document(doc)
        }
    }
}

/// Converts a JSON `_id` value to a BSON value, trying ObjectId first.
fn parse_bson_id(val: &JsonValue) -> Bson {
    if let JsonValue::String(s) = val {
        if let Ok(oid) = bson::oid::ObjectId::parse_str(s) {
            return Bson::ObjectId(oid);
        }
        return Bson::String(s.clone());
    }
    json_to_bson(val)
}

/// Collects unique column names from a slice of documents,
/// placing `_id` first and preserving insertion order for the rest.
fn collect_columns(docs: &[Document]) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut columns: Vec<String> = Vec::new();

    // _id first if present in any document
    for doc in docs {
        if doc.contains_key("_id") && !seen.contains("_id") {
            seen.insert("_id".to_string());
            columns.push("_id".to_string());
            break;
        }
    }

    for doc in docs {
        for k in doc.keys() {
            if !seen.contains(k.as_str()) {
                seen.insert(k.clone());
                columns.push(k.clone());
            }
        }
    }

    columns
}

/// Converts a slice of BSON documents to a JSON row matrix.
fn docs_to_rows(docs: &[Document], columns: &[String]) -> Vec<JsonValue> {
    docs.iter()
        .map(|doc| {
            let row: Vec<JsonValue> = columns
                .iter()
                .map(|k| {
                    doc.get(k.as_str())
                        .map(bson_to_json)
                        .unwrap_or(JsonValue::Null)
                })
                .collect();
            JsonValue::Array(row)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_from_clause_strips_identifier_quotes() {
        assert_eq!(
            parse_sql_from_clause("SELECT * FROM \"plans\""),
            Some("plans".to_string())
        );
        assert_eq!(
            parse_sql_from_clause("SELECT * FROM `plans` LIMIT 10"),
            Some("plans".to_string())
        );
        assert_eq!(
            parse_sql_from_clause("SELECT * FROM plans"),
            Some("plans".to_string())
        );
    }

    #[test]
    fn full_srv_uri_is_returned_unchanged() {
        let uri = "mongodb+srv://user:password@cluster0.example.mongodb.net/mydb?retryWrites=true&w=majority&authSource=admin";
        let params = json!({ "connection_string": uri });

        assert_eq!(build_uri(&params).unwrap(), uri);
    }

    #[test]
    fn full_standard_uri_with_multiple_hosts_is_returned_unchanged() {
        let uri = "mongodb://host1:27017,host2:27017,host3:27017/mydb?tls=true&authSource=admin&replicaSet=atlas-example-shard-0";
        let params = json!({ "connectionUri": uri });

        assert_eq!(build_uri(&params).unwrap(), uri);
    }

    #[test]
    fn database_array_uses_first_entry() {
        // Multi-database drivers receive `database` as an array of selected
        // names; the first one is the initial database.
        let params = json!({ "host": "localhost", "database": ["finance", "audit"] });
        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://localhost:27017/finance"
        );

        let empty = json!({ "host": "localhost", "database": [] });
        assert_eq!(
            build_uri(&empty).unwrap(),
            "mongodb://localhost:27017/admin"
        );
    }

    #[test]
    fn connection_uri_param_wins_over_decomposed_fields() {
        // `connection_uri` is the exact key the Tabularis host forwards for
        // URI-passthrough drivers; the decomposed fields it sends alongside are
        // display-only and must not influence the URI.
        let uri = "mongodb+srv://user:password@cluster0.example.mongodb.net/?tls=true&retryWrites=true&w=majority&appName=Cluster0";
        let params = json!({
            "connection_uri": uri,
            "host": "cluster0.example.mongodb.net",
            "port": 27017,
            "username": "user",
            "database": "",
        });

        assert_eq!(build_uri(&params).unwrap(), uri);
    }

    #[test]
    fn uri_database_is_used_when_explicit_database_is_absent() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let client = runtime
            .block_on(Client::with_uri_str("mongodb://localhost/finance"))
            .unwrap();
        let default_database = client.default_database();

        assert_eq!(
            resolve_database_name(
                None,
                default_database.as_ref().map(|database| database.name())
            ),
            "finance"
        );
    }

    #[test]
    fn explicit_database_overrides_uri_database() {
        assert_eq!(
            resolve_database_name(Some("reporting"), Some("finance")),
            "reporting"
        );
    }

    #[test]
    fn admin_is_used_when_no_database_is_configured() {
        assert_eq!(resolve_database_name(None, None), "admin");
    }

    #[test]
    fn non_string_database_is_rejected() {
        let params = json!({ "database": 42 });
        let error = get_string_param(&params, "database").unwrap_err();

        assert_eq!(error, "Parameter 'database' must be a string");
    }

    #[test]
    fn host_port_credentials_and_database_build_a_compatible_uri() {
        let params = json!({
            "host": "localhost",
            "port": 27018,
            "database": "finance",
            "username": "analyst",
            "password": "secret"
        });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://analyst:secret@localhost:27018/finance"
        );
    }

    #[test]
    fn credentials_are_percent_encoded_when_building_a_uri() {
        let params = json!({
            "host": "localhost",
            "username": "bank/user",
            "password": "p@ss/w#rd:$",
            "database": "finance"
        });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://bank%2Fuser:p%40ss%2Fw%23rd%3A%24@localhost:27017/finance"
        );
    }

    #[test]
    fn auth_source_is_appended_to_a_built_uri() {
        let params = json!({
            "host": "localhost",
            "database": "finance",
            "authSource": "admin"
        });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://localhost:27017/finance?authSource=admin"
        );
    }

    #[test]
    fn tls_is_appended_to_a_built_uri() {
        let params = json!({ "host": "localhost", "tls": true });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://localhost:27017/admin?tls=true"
        );
    }

    #[test]
    fn replica_set_is_appended_to_a_built_uri() {
        let params = json!({
            "host": "localhost",
            "replicaSet": "atlas-xxxxx-shard-0"
        });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://localhost:27017/admin?replicaSet=atlas-xxxxx-shard-0"
        );
    }

    #[test]
    fn unsupported_uri_scheme_returns_a_secret_free_error() {
        let params = json!({
            "connection_string": "postgresql://banker:top-secret@example.com/finance"
        });

        let error = build_uri(&params).unwrap_err();

        assert_eq!(
            error,
            "Unsupported MongoDB URI scheme; expected mongodb:// or mongodb+srv://"
        );
        assert!(!error.contains("top-secret"));
    }

    #[test]
    fn extra_options_are_sorted_and_percent_encoded() {
        let params = json!({
            "host": "localhost",
            "extra_options": {
                "zOption": "value with spaces",
                "compressors": "zstd"
            }
        });

        assert_eq!(
            build_uri(&params).unwrap(),
            "mongodb://localhost:27017/admin?compressors=zstd&zOption=value%20with%20spaces"
        );
    }

    #[test]
    fn mongo_uri_is_fully_redacted() {
        let uri = "connection failed for mongodb+srv://banker:top-secret@cluster.example.mongodb.net/finance";

        let masked = mask_mongo_uri(uri);

        assert_eq!(masked, "connection failed for mongodb+srv://<redacted>");
        assert!(!masked.contains("banker"));
        assert!(!masked.contains("top-secret"));
    }

    #[test]
    fn malformed_uri_delimiters_and_query_values_are_redacted() {
        let uri = "mongodb://banker:pa/ss?token=secret#fragment@host/finance?apiKey=hidden";

        let masked = mask_mongo_uri(uri);

        assert_eq!(masked, "mongodb://<redacted>");
        assert!(!masked.contains("secret"));
        assert!(!masked.contains("hidden"));
    }

    #[test]
    fn quoted_uri_with_whitespace_is_redacted_through_the_closing_quote() {
        let error =
            "failure: \"mongodb://user:bad password@host/finance?apiKey=hidden\" retry later";

        assert_eq!(
            mask_mongo_uri(error),
            "failure: \"mongodb://<redacted>\" retry later"
        );
    }

    #[test]
    fn multiple_uris_and_percent_encoded_credentials_are_redacted() {
        let error = "first mongodb://banker:p%40ss@host/finance?token=one second mongodb+srv://user:secret@cluster/ledger?token=two";
        let masked = mask_mongo_uri(error);

        assert_eq!(
            masked,
            "first mongodb://<redacted> second mongodb+srv://<redacted>"
        );
        for secret in ["banker", "p%40ss", "one", "user", "secret", "two"] {
            assert!(!masked.contains(secret));
        }
    }
}
