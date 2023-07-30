package com.sanjith.dbmigrator.dao;


import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


import java.util.LinkedHashSet;
import java.util.Map;
import java.util.List;

import javax.sql.DataSource;

/**
 * DAO Class for Source table
 */
@Component
public class Source {

    @Value("${input.source-table}")
    private String tableName;

    @Value("${input.source-schema}")
    private String schemaName;

    @Qualifier("destinationDataSource")
    @Autowired
    private DataSource source;

    @Qualifier("sourceTemplate")
    @Autowired
    private JdbcTemplate template;

    private Map<String, Integer> columnMetaData = null;


    /**
     * Validates the table name and schema name
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
     * @return column metadata in map of column name and its java.sql Type
     */
    public Map<String, Integer> getColumnMetaData() {
        return columnMetaData;
    }

    /**
     * Set the column metadata
     * @param columnMetaData map of column name and its java.sql Type
     */
    public void setColumnMetaData(Map<String, Integer> columnMetaData) {
        this.columnMetaData = columnMetaData;
    }


    /**
     * Get the record count of the table
     * @return row count
     */
    public long getRecordCount() {
        System.out.println("Querying record count of " + this.tableName);
        Integer result = template.queryForObject("SELECT COUNT(*) FROM " + tableName, Integer.class);
        if (result == null) {
            return -1;
        }

        return result;
    }

    /**
     * Get the data source
     * @return data source
     */
    public DataSource getDataSource() {
        return this.source;
    }

}
