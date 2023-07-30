package com.sanjith.dbmigrator.dao;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for DB operations
 * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html">Database metadata</a>
 */
public class DbUtils {

    /**
     * Validates the table name - Searches for table in db
     * @param schema Schema name
     * @param table Table name
     * @param template Jdbc template
     * @return true - Valid table, false - Invalid table
     */
    public static boolean isValidTable(String schema, String table, JdbcTemplate template) {
        boolean result = Boolean.TRUE.equals(template.execute(new ConnectionCallback<Boolean>() {
            @Override
            public Boolean doInConnection(Connection con) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet rs = metaData.getTables(null, schema, table, new String[]{"TABLE"});

                if (!rs.next()) {
                    return false;
                } else {
                    return true;
                }
            }
        }));

        return result;
    }

    /**
     * Validates the schema name - Searches for schema in db
     * @param schema Schema name
     * @param template Jdbc template
     * @return true - Valid schema, false - Invalid schema
     */
    public static boolean isValidSchema(String schema, JdbcTemplate template) {
        boolean result = Boolean.TRUE.equals(template.execute(new ConnectionCallback<Boolean>() {
            @Override
            public Boolean doInConnection(Connection con) throws SQLException, DataAccessException {
                DatabaseMetaData metaData = con.getMetaData();
                ResultSet rs = metaData.getSchemas(null, schema);

                if (!rs.next()) {
                    return false;
                } else {
                    return true;
                }
            }
        }));

        return result;
    }

    /**
     * Retrieves the column name and types
     * @param schema schema name
     * @param table table name
     * @param template jdbc template
     * @return Key - column name. Value - Column datatype in SQL type form java.sql.Types
     * @throws RuntimeException
     */
    public static Map<String, Integer> getColumnNamesTypes(String schema, String table, JdbcTemplate template) throws RuntimeException {
        Map<String, Integer> result = template.execute((ConnectionCallback<Map>) connection -> {

            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getColumns(null, schema, table, null);
            Map<String, Integer> columnEntry = new HashMap<>();

            while (rs.next()) {
                if (rs.getInt("DATA_TYPE") == Types.CLOB || rs.getInt("DATA_TYPE") == Types.BLOB) {
                    throw new RuntimeException("CLOB/BLOB detected. Migration could not proceed");
                }
                columnEntry.put(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"));
            }
            return columnEntry;

        });

        return result;
    }


}
