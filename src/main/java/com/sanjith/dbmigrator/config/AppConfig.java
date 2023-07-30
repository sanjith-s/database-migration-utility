package com.sanjith.dbmigrator.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Application config class
 */

@Configuration
@EnableTransactionManagement
public class AppConfig {

    @Value("${input.poolSize")
    private int poolSize;

    /**
     * Creates datasource bean for Source db. Uses HikariCP datasource
     *
     * @return Datasource for source db
     */
    @Bean(name = "sourceDataSource")
    @ConfigurationProperties(prefix = "source")
    public DataSource dbSource() {
        HikariDataSource hds = DataSourceBuilder.create().type(com.zaxxer.hikari.HikariDataSource.class).build();
        hds.setConnectionTimeout(300000);
        hds.setPoolName("source-pool");
        return hds;
    }

    /**
     * Creates datasource bean for Destination db. Uses HikariCP datasource
     *
     * @return Datasource for Destination db
     */
    @Bean(name = "destinationDataSource")
    @ConfigurationProperties(prefix = "destination")
    public DataSource dbDestination() {
        HikariDataSource hds = DataSourceBuilder.create().type(com.zaxxer.hikari.HikariDataSource.class).build();
        hds.setConnectionTimeout(300000);
        hds.setMaximumPoolSize(poolSize);
        hds.setPoolName("destination-pool");
        return hds;
    }

    /**
     * Creates transactional manager bean for handling transactions
     *
     * @return Transaction manager for destination DB
     */
    @Bean
    public PlatformTransactionManager transactionManager() {
        return new DataSourceTransactionManager((dbDestination()));
    }

    /**
     * Creates JdbcTemplate Bean for destination DB
     *
     * @return JdbcTemplate for destination DB
     */
    @Bean(name = "destinationTemplate")
    public JdbcTemplate destinationTemplate() {
        return new JdbcTemplate(dbDestination());
    }

    /**
     * Creates JdbcTemplate Bean for source DB
     *
     * @return JdbcTemplate for source DB
     */
    @Bean(name = "sourceTemplate")
    public JdbcTemplate sourceTemplate() {
        return new JdbcTemplate(dbSource());
    }

}
