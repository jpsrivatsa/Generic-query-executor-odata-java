# Generic-query-executor-odata-java
Generic SQL query executor in springboot for odata like filters

Lightweight, standalone Java library for executing generic SQL queries and updates using JDBC. 
It provides a clean, type-safe API for filtering, pagination, ordering, and mapping results to Java objects or `Map<String, Object>`.  
It currently works for POSTGRES only! I will update to other DBs soon.

This library is **battle-tested** in one of our production applications and has been handling complex queries and updates reliably for thousands of records. 
It is designed to be reusable across projects, easy to integrate, and requires **no Spring dependencies** — just a standard JDBC connection.

## Features

- Fully generic **SELECT** queries with optional:
  - Field selection
  - Filtering (`eq`, `ne`, `gt`, `lt`, `ge`, `le`, `like`, `in`, `between`, date range)
  - Logical AND / OR conditions
  - Ordering
  - Pagination (skip & top)
- Generic **UPDATE** operations with automatic `changed_at` timestamp
- Supports `DATE` and `TIMESTAMP` fields for proper parameter binding
- Flexible **RowMapper** interface:
  - Use default `Map<String,Object>` mapper or implement custom POJO mapping
- Easy-to-use **Filter** object with helper methods (`eq()`, `like()`, `in()`, `between()`, etc.)  

## Usage Example

Connection conn = DriverManager.getConnection(url, user, pass);
Map<String,String> allowedFields = Map.of("id","id","name","name","createdAt","created_at");

GenericDatabaseExecutor<Map<String,Object>> executor =
    new GenericDatabaseExecutor<>(conn, allowedFields, Set.of(), Set.of("createdAt"), GenericDatabaseExecutor.mapRowToMap());

// Create filters
var filter1 = GenericDatabaseExecutor.Filter.like("name","John");
var filter2 = GenericDatabaseExecutor.Filter.gt("createdAt","2025-01-01").or();

List<GenericDatabaseExecutor.Filter> filters = List.of(filter1, filter2);

// Execute query
var result = executor.query("users", List.of("id","name"), filters, "createdAt DESC", 0, 50);

// Execute update
executor.update("users", "id", Map.of("name","John Updated"), 101);
