import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * GenericDatabaseExecutor - standalone library for querying and updating SQL databases using JDBC.
 */
public class GenericDatabaseExecutor<T> {

    private final Connection connection;
    private final Map<String, String> allowedFields;
    private final Set<String> dateFields;
    private final Set<String> timestampFields;
    private final RowMapper<T> rowMapper;

    // map roes
    public interface RowMapper<R> {
        R mapRow(ResultSet rs) throws SQLException;
    }

    // filtering --> add as necessary
    public static class Filter {
        private final String field;
        private final Operator operator;
        private final Object value;
        private final List<Object> values;
        private final Logic logic;

        public enum Operator { EQ, NE, GT, LT, GE, LE, LIKE, IN, BETWEEN, DTG, DTL }
        public enum Logic { AND, OR }

        // define filter constructor logic
        private Filter(String field, Operator op, Object value, List<Object> values, Logic logic) {
            this.field = field;
            this.operator = op;
            this.value = value;
            this.values = values;
            this.logic = logic;
        }

        
        public static Filter eq(String field, Object value) {
            return new Filter(field, Operator.EQ, value, null, Logic.AND);
        }

        public static Filter ne(String field, Object value) {
            return new Filter(field, Operator.NE, value, null, Logic.AND);
        }

        public static Filter gt(String field, Object value) {
            return new Filter(field, Operator.GT, value, null, Logic.AND);
        }

        public static Filter lt(String field, Object value) {
            return new Filter(field, Operator.LT, value, null, Logic.AND);
        }

        public static Filter ge(String field, Object value) {
            return new Filter(field, Operator.GE, value, null, Logic.AND);
        }

        public static Filter le(String field, Object value) {
            return new Filter(field, Operator.LE, value, null, Logic.AND);
        }

        public static Filter like(String field, String pattern) {
            return new Filter(field, Operator.LIKE, pattern, null, Logic.AND);
        }

        public static Filter between(String field, Object start, Object end) {
            return new Filter(field, Operator.BETWEEN, null, List.of(start, end), Logic.AND);
        }

        public static Filter in(String field, List<Object> values) {
            return new Filter(field, Operator.IN, null, values, Logic.AND);
        }

        public static Filter dtg(String field, String interval) {
            return new Filter(field, Operator.DTG, interval, null, Logic.AND);
        }

        public static Filter dtl(String field, String interval) {
            return new Filter(field, Operator.DTL, interval, null, Logic.AND);
        } // date less than today (like days, months, years)

        public Filter or() {
            return new Filter(this.field, this.operator, this.value, this.values, Logic.OR);
        }
    }

    //use same way in code and define a connection, fields you want to allow and fields that are datetime and timestamps
    public GenericDatabaseExecutor(Connection connection,
                                   Map<String, String> allowedFields,
                                   Set<String> dateFields,
                                   Set<String> timestampFields,
                                   RowMapper<T> rowMapper) {
        this.connection = connection;
        this.allowedFields = allowedFields;
        this.dateFields = dateFields != null ? dateFields : Collections.emptySet();
        this.timestampFields = timestampFields != null ? timestampFields : Collections.emptySet();
        this.rowMapper = rowMapper;
    }

    //this lets u universally compute query from any module/submodule
    public QueryResult<T> query(String tableName,
                                List<String> selectFields,
                                List<Filter> filters,
                                String orderBy,
                                int skip,
                                int top) throws SQLException {

        StringBuilder sql = new StringBuilder(" FROM ").append(tableName).append(" WHERE 1=1 ");
        List<Object> params = new ArrayList<>();
        List<String> paramFields = new ArrayList<>();

        if (filters != null) {
            for (Filter f : filters) {
                String col = allowedFields.get(f.field);
                if (col == null) throw new IllegalArgumentException("Invalid field: " + f.field);

                String logic = f.logic == Filter.Logic.OR ? " OR " : " AND ";
                sql.append(logic);

                switch (f.operator) {
                    case EQ -> sql.append(col).append(" = ?");
                    case NE -> sql.append(col).append(" <> ?");
                    case GT -> sql.append(col).append(" > ?");
                    case LT -> sql.append(col).append(" < ?");
                    case GE -> sql.append(col).append(" >= ?");
                    case LE -> sql.append(col).append(" <= ?");
                    case LIKE -> sql.append("LOWER(").append(col).append(") LIKE LOWER(?)");
                    case BETWEEN -> sql.append(col).append(" BETWEEN ? AND ?");
                    case IN -> sql.append(col).append(" IN (").append(inPlaceholders(f.values)).append(")");
                    case DTG -> sql.append(col).append(" BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '").append(f.value).append("'");
                    case DTL -> sql.append(col).append(" BETWEEN CURRENT_DATE - INTERVAL '").append(f.value).append("' AND CURRENT_DATE");
                }

                if (f.operator == Filter.Operator.LIKE) {
                    params.add("%" + f.value + "%");
                    paramFields.add(f.field);
                } else if (f.operator == Filter.Operator.BETWEEN) {
                    params.add(f.values.get(0));
                    params.add(f.values.get(1));
                    paramFields.add(f.field);
                    paramFields.add(f.field);
                } else if (f.operator == Filter.Operator.IN) {
                    for (Object v : f.values) {
                        params.add(v);
                        paramFields.add(f.field);
                    }
                } else if (f.operator != Filter.Operator.DTG && f.operator != Filter.Operator.DTL) {
                    params.add(f.value);
                    paramFields.add(f.field);
                }
            }
        }

        // actual select clause built for POSTGRES only at the moment
        String selectClause = selectFields != null && !selectFields.isEmpty()
                ? selectFields.stream().map(f -> {
                    if (!allowedFields.containsKey(f)) throw new IllegalArgumentException(f);
                    return allowedFields.get(f);
                }).collect(Collectors.joining(", "))
                : String.join(",", allowedFields.values());

        //order clause for ascending and descending filters
        String orderClause = "";
        if (orderBy != null && !orderBy.isBlank()) {
            String[] parts = orderBy.trim().split("\\s+");
            String field = parts[0];
            String dir = parts.length > 1 ? parts[1].toUpperCase() : "ASC";
            if (!allowedFields.containsKey(field)) throw new IllegalArgumentException(field);
            if (!dir.equals("ASC") && !dir.equals("DESC")) throw new IllegalArgumentException("Invalid sort: " + dir);
            orderClause = " ORDER BY " + allowedFields.get(field) + " " + dir;
        }

        String limitOffset = " LIMIT " + (top > 0 ? top : 25) + (skip > 0 ? " OFFSET " + skip : "");
        String dataSql = "SELECT " + selectClause + sql + orderClause + limitOffset;
        String countSql = "SELECT COUNT(*)" + sql;

        List<T> data = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(dataSql)) {
            setParams(ps, params, paramFields);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) data.add(rowMapper.mapRow(rs));
            }
        }

        long total = 0;
        try (PreparedStatement ps = connection.prepareStatement(countSql)) {
            setParams(ps, params, paramFields);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) total = rs.getLong(1);
            }
        }

        return new QueryResult<>(data, total);
    }

    private void setParams(PreparedStatement ps, List<Object> params, List<String> paramFields) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object val = params.get(i);
            String field = paramFields.get(i);

            if (val != null && val instanceof String) {
                if (dateFields.contains(field)) ps.setDate(i + 1, Date.valueOf((String) val));
                else if (timestampFields.contains(field)) ps.setTimestamp(i + 1, Timestamp.valueOf((String) val));
                else ps.setObject(i + 1, val);
            } else {
                ps.setObject(i + 1, val);
            }
        }
    }

    private String inPlaceholders(List<Object> values) {
        if (values == null || values.isEmpty()) throw new IllegalArgumentException("IN values empty");
        return values.stream().map(v -> "?").collect(Collectors.joining(", "));
    }

    // update query --> caution! u need to handle transaction in the consuming class to keep consistency and 2PC compliance
    public void update(String tableName, String pkColumn, Map<String, Object> changes, Object pkValue) throws SQLException {
        if (changes.isEmpty()) throw new IllegalArgumentException("No changes");

        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        List<Object> params = new ArrayList<>();
        for (Map.Entry<String, Object> e : changes.entrySet()) {
            sql.append(e.getKey()).append(" = ?, ");
            params.add(e.getValue());
        }
        sql.append("changed_at = ?");
        params.add(Timestamp.valueOf(LocalDateTime.now()));
        sql.append(" WHERE ").append(pkColumn).append(" = ?");
        params.add(pkValue);

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            int rows = ps.executeUpdate();
            if (rows == 0) throw new SQLException("No rows updated");
        }
    }


    public static class QueryResult<R> {
        public final List<R> data;
        public final long total;

        public QueryResult(List<R> data, long total) {
            this.data = data;
            this.total = total;
        }
    }

    //map returning rows
    public static RowMapper<Map<String,Object>> mapRowToMap() {
        return rs -> {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            Map<String,Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= count; i++) {
                row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
            }
            return row;
        };
    }
}
