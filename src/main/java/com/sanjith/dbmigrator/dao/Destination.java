package com.sanjith.dbmigrator.dao;

import static java.sql.Statement.EXECUTE_FAILED;

import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.BatchUpdateException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.List;

import javax.sql.DataSource;

/**
 * DAO Class for Destination table
 */
@Component
public class Destination {

    @Value("${input.destination-table}")
    private String tableName;

    @Value("${input.destination-schema}")
    private String schemaName;

    @Qualifier("destinationDataSource")
    @Autowired
    private DataSource destination;

    @Qualifier("destinationTemplate")
    @Autowired
    private JdbcTemplate template;

    private Map<String, Integer> columnMetaData = null;

    /**
     * Inserts queried data into destination table
     * @param columnNames Column names
     * @param valueList Column types
     * @param typeArr Column types
     * @return Status of insertion (true - SUCCESS, false - FAILURE)
     * @see DbUtils
     * @see Source
     */
    public int insertData(LinkedHashSet<String> columnNames, List<Object[]> valueList, int[] typeArr) {

        StringBuilder stmtBuilder = new StringBuilder("INSERT INTO ");
        stmtBuilder.append(tableName);
        stmtBuilder.append(" (");
        stmtBuilder.append(String.join(",", columnNames));
        stmtBuilder.append(") VALUES (");
        stmtBuilder.append(Strings.repeat("?,", columnNames.size() - 1));
        stmtBuilder.append("?)");

        String sqlStmt = stmtBuilder.toString();
        int queryResult[] = null;

        try {
            queryResult = template.batchUpdate(sqlStmt, valueList, typeArr);
        } catch (Exception e) {
            BatchUpdateException ex = (BatchUpdateException) e.getCause();
            Exception eex = ex.getNextException();
            eex.printStackTrace();

            throw new RuntimeException("Insert Failed");
        }

        for (int insertStatus : queryResult) {
            if (insertStatus == EXECUTE_FAILED) {
                throw new RuntimeException("Insert Failed");
            }
        }

        return valueList.size();
    }

    /**
     * Truncates the destination table
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void clearRecords() {
        System.out.println("Truncating destination");
        template.execute("DELETE FROM " + tableName);
    }

    /**
     * Truncates the destination table. Requires the existing transaction
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void clearRecordsTransactional() {
        System.out.println("Truncating destination");
        template.execute("DELETE FROM " + tableName);
    }

    /**
     * Verifies the table name and schema name
     * @throws IllegalArgumentException
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void verify() throws IllegalArgumentException {

        if (DbUtils.isValidSchema(schemaName, template) == false) {
            throw new IllegalArgumentException("Invalid Schema/ Schema Not found");
        }

        if (DbUtils.isValidTable(schemaName, tableName, template) == false) {
            throw new IllegalArgumentException("Invalid Table/ Table not found");
        }

        this.setColumnMetaData(DbUtils.getColumnNamesTypes(schemaName, tableName, template));
    }


    /**
     * Get the table name
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the schema name
     * @return schema name
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Get the column metadata
     * @return Column metadata
     * @see DbUtils
     */
    public Map<String, Integer> getColumnMetaData() {
        return columnMetaData;
    }

    /**
     * Set the column metadat
     * @param columnMetaData
     * @see DbUtils
     */
    public void setColumnMetaData(Map<String, Integer> columnMetaData) {
        this.columnMetaData = columnMetaData;
    }

    /**
     * Get the connection pool size
     * @return Connection pool size
     */
    public int getPoolSize() {
        return ((HikariDataSource) this.destination).getMaximumPoolSize();
    }
}
