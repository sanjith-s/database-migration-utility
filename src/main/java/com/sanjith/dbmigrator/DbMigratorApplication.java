package com.sanjith.dbmigrator;

import com.google.common.base.Throwables;
import com.sanjith.dbmigrator.service.DataMigrator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Spring boot application class for Data Migration
 *
 * @author Sanjith S
 */
@SpringBootApplication
public class DbMigratorApplication implements CommandLineRunner {

    private static final Logger LOG = LogManager.getLogger(DbMigratorApplication.class);

    private final DataMigrator serviceManager;

    @Value("${input.multithreaded}")
    private boolean multithreaded;

    @Value("${input.banner}")
    private boolean banner;

    @Value("${input.bannerOpt}")
    private int bannerOpt;


    private final String BANNER = " ______  _______ _______ _______ ______  _______ _______ _______      _______ _____  ______  ______ _______ _______  _____   ______\n"
+" |     \\ |_____|    |    |_____| |_____] |_____| |______ |______      |  |  |   |   |  ____ |_____/ |_____|    |    |     | |_____/\n"
        +" |_____/ |     |    |    |     | |_____] |     | ______| |______      |  |  | __|__ |_____| |    \\_ |     |    |    |_____| |    \\_\n";


    public DbMigratorApplication(@Autowired DataMigrator serviceManager) {
        this.serviceManager = serviceManager;
    }

    public static void main(String[] args) {
        SpringApplication.run(DbMigratorApplication.class, args);
    }

    /**
     * Display the banner, version and build timestamp
     */
    private void printBanner() {
        String version = null;
        String build = null;

        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream("META/MANIFEST.MF");
            Properties prop = new Properties();
            prop.load(is);
            version = prop.getProperty("Implementation-Version");
            build = prop.getProperty("Build-Time");

        } catch (IOException e) {
            LOG.warn("Error in accessing Manifest file");
        }

        System.out.println(BANNER);

        if(version != null && build != null) {
            System.out.println("Version: " + version);
            System.out.println("Build: " + build + "\n");
        }
    }

    @Override
    public void run(String... args) {

        boolean EXIT_STATUS = false;

        try {
            if(banner) printBanner();

            long startTime = System.currentTimeMillis();
            LOG.info("Service started");

            if(multithreaded) {
                LOG.info("Executing migration task in multithreaded env");
                EXIT_STATUS = serviceManager.migrateDataMT();
            } else {
                LOG.info("Executing migration task in single threaded env");
                EXIT_STATUS = serviceManager.migrateDataST();
            }

            long endTime = System.currentTimeMillis();
            LOG.info("Service completed, Time elapsed " + (endTime-startTime) + " MS");
            System.out.println("Service completed, Time elapsed " + (endTime-startTime) + " MS");

        } catch (Exception e) {
            LOG.error("Unknown error" + Throwables.getStackTraceAsString(e));
        } finally {
             if(EXIT_STATUS == true) {
                 LOG.info("Migration Success");
                 System.out.println("Migration Success\n");
             } else {
                 LOG.error("Error in Migration");
                 System.out.println("Error in migration\n");
             }
        }

    }
}
