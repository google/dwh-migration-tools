package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Minimal Databricks HTTP client for Unity Catalog and SQL Statement Execution endpoints. */
public class DatabricksClient {
  private static final String SQL_STATEMENTS_PATH = "/api/2.0/sql/statements";
  private final String baseUrl;
  private final String token;
  private final String warehouseId;
  private final ObjectMapper mapper = new ObjectMapper();

  public DatabricksClient(String baseUrl, String token) {
    this(baseUrl, token, null);
  }

  public DatabricksClient(String baseUrl, String token, String warehouseId) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.token = token;
    this.warehouseId = warehouseId;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public List<Map<String, Object>> listCatalogs() throws IOException {
    JsonNode root = getJson("/api/2.1/unity-catalog/catalogs");
    return nodeToListOfMaps(root, "catalogs");
  }

  public List<Map<String, Object>> listSchemas(String catalogName) throws IOException {
    String q = "?catalog_name=" + urlEncode(catalogName);
    JsonNode root = getJson("/api/2.1/unity-catalog/schemas" + q);
    return nodeToListOfMaps(root, "schemas");
  }

  public List<Map<String, Object>> listTables(String catalogName, String schemaName) throws IOException {
    String q = "?catalog_name=" + urlEncode(catalogName) + "&schema_name=" + urlEncode(schemaName);
    JsonNode root = getJson("/api/2.1/unity-catalog/tables" + q);
    return nodeToListOfMaps(root, "tables");
  }

  public boolean hasWarehouse() {
    return warehouseId != null && !warehouseId.isEmpty();
  }

  public List<Map<String, Object>> listHiveMetastoreSchemas() throws IOException {
    return executeSql("SHOW SCHEMAS IN hive_metastore");
  }

  public List<Map<String, Object>> listHiveMetastoreTables(String schemaName) throws IOException {
    return executeSql("SHOW TABLES IN hive_metastore." + quoteIdentifier(schemaName));
  }

  public List<Map<String, Object>> describeHiveMetastoreTable(
      String schemaName, String tableName) throws IOException {
    return executeSql(
        "DESCRIBE TABLE hive_metastore."
            + quoteIdentifier(schemaName)
            + "."
            + quoteIdentifier(tableName));
  }

  private List<Map<String, Object>> executeSql(String statement) throws IOException {
    if (!hasWarehouse()) {
      throw new IOException("Databricks SQL warehouse is required for Hive Metastore queries");
    }
    HttpURLConnection conn = openConnection(SQL_STATEMENTS_PATH, "POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    Map<String, Object> request = new HashMap<>();
    request.put("warehouse_id", warehouseId);
    request.put("statement", statement);
    request.put("wait_timeout", "50s");
    request.put("disposition", "INLINE");
    request.put("format", "JSON_ARRAY");
    try (OutputStream output = conn.getOutputStream()) {
      mapper.writeValue(output, request);
    }
    JsonNode root = readResponse(conn);
    String statementId = textAt(root, "statement_id");
    String state = textAt(root, "status", "state");
    while (statementId != null && ("PENDING".equals(state) || "RUNNING".equals(state))) {
      conn = openConnection(SQL_STATEMENTS_PATH + "/" + urlEncode(statementId), "GET");
      root = readResponse(conn);
      state = textAt(root, "status", "state");
    }
    if (!"SUCCEEDED".equals(state)) {
      throw new IOException("Databricks SQL statement failed: " + textAt(root, "status", "error"));
    }
    return rows(root);
  }

  private List<Map<String, Object>> rows(JsonNode root) {
    JsonNode columns = root.at("/manifest/schema/columns");
    if (!columns.isArray()) columns = root.at("/result/schema/columns");
    JsonNode data = root.at("/result/data_array");
    if (!data.isArray()) return Collections.emptyList();
    List<Map<String, Object>> rows = new ArrayList<>();
    for (JsonNode row : data) {
      Map<String, Object> values = new HashMap<>();
      for (int i = 0; i < row.size(); i++) {
        String name = columns.isArray() && i < columns.size()
            ? columns.get(i).path("name").asText("column_" + i)
            : "column_" + i;
        values.put(name, mapper.convertValue(row.get(i), Object.class));
      }
      rows.add(values);
    }
    return rows;
  }

  private String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private String textAt(JsonNode root, String... path) {
    JsonNode value = root;
    for (String part : path) value = value.path(part);
    return value.isMissingNode() || value.isNull() ? null : value.asText();
  }

  private String urlEncode(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  private JsonNode getJson(String path) throws IOException {
    HttpURLConnection conn = openConnection(path, "GET");
    return readResponse(conn);
  }

  private HttpURLConnection openConnection(String path, String method) throws IOException {
    URL url = new URL(baseUrl + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);
    if (token != null && !token.isEmpty()) {
      conn.setRequestProperty("Authorization", "Bearer " + token);
    }
    conn.setRequestProperty("Accept", "application/json");
    return conn;
  }

  private JsonNode readResponse(HttpURLConnection conn) throws IOException {
    int code = conn.getResponseCode();
    InputStream is = (code >= 200 && code <= 299) ? conn.getInputStream() : conn.getErrorStream();
    try (BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String body = r.lines().collect(Collectors.joining("\n"));
      if (code < 200 || code > 299) {
        throw new IOException("Databricks API request failed: " + code + " " + body);
      }
      return mapper.readTree(body);
    } finally {
      conn.disconnect();
    }
  }

  private List<Map<String, Object>> nodeToListOfMaps(JsonNode node, String fieldName) {
    if (node == null || node.isNull()) return Collections.emptyList();
    JsonNode arr = node;
    if (node.has(fieldName)) {
      arr = node.get(fieldName);
    }
    if (arr == null || !arr.isArray()) return Collections.emptyList();
    List<Map<String, Object>> out = new ArrayList<>();
    Iterator<JsonNode> it = arr.elements();
    while (it.hasNext()) {
      JsonNode item = it.next();
      Map<String, Object> map = mapper.convertValue(item, Map.class);
      out.add(map);
    }
    return out;
  }
}
