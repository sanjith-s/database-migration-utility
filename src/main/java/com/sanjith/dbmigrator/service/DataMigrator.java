package com.sanjith.dbmigrator.service;


import com.google.common.base.Throwables;
import com.sanjith.dbmigrator.dao.Destination;
import com.sanjith.dbmigrator.dao.Source;
import dnl.utils.text.table.TextTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;


/**
 * Service which orchestrates the migration of data
 */

@Service
public class DataMigrator {

    private static final Logger log = LogManager.getLogger(DataMigrator.class);

    /**
     * Upperbound for iterations in batch processing
     */
    private static final int MAX_ITERATIONS = 999999;
    private static final long WARN_THRESHOLD = 1000000;
    /**
     * Specifies the batch size of the migration task
     */
    @Value("${input.batch-size}")
    public int BATCH_SIZE;

    /**
     * true - Batched operation
     * false - Non-batched operation
     */
    @Value("${input.batched}")
    public boolean isBatched;
    @Value("${input.append}")
    private boolean appendToDestination;

    @Autowired
    private Destination destinationTable;

    @Autowired
    private Source sourceTable;


    /**
     * Column Names
     */
    protected LinkedHashSet<String> columnNames;

    /**
     * Column types in java.sql.Types
     */
    protected int[] typeArr;

    /**
     * Migrates data in sequential querying and inserts. Entire migration will be done in a single transaction. Make sure the transaction log file is large enough at destination DB
     *
     * @return status of migration (true SUCCESS,false FAILURE)
     */
    @Transactional
    public boolean migrateDataST() throws Exception {
        try {
            try {
                sourceTable.verify();
                destinationTable.verify();
            } catch (IllegalArgumentException e) {
                log.error(e.getMessage() + "In" + Throwables.getStackTraceAsString(e));
                return false;
            } catch (RuntimeException e) {
                log.error(e.getMessage());
                return false;
            }

            long recordCount = sourceTable.getRecordCount();
            if (recordCount > WARN_THRESHOLD) {
                try {
                    warnUser(recordCount);
                } catch (IllegalArgumentException e) {
                    log.error(e.toString());
                    return false;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    log.info(e.toString());
                    return false;
                }
            }
            if (appendToDestination == false) {
                destinationTable.clearRecords();
            }
            long startTime = System.currentTimeMillis();
            HikariDataSource srcDatasrc = (HikariDataSource) sourceTable.getDataSource();
            Connection srcConn = null;

            try {
                try {
                    srcConn = srcDatasrc.getConnection();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    log.error(Throwables.getStackTraceAsString(e));
                    throw new RuntimeException("Error in fetching connection to source DB");
                }

                Statement stmt = srcConn.createStatement();
                stmt.setFetchSize(BATCH_SIZE);
                Map<String, Integer> columnMetaData = destinationTable.getColumnMetaData();
                LinkedHashSet<String> columnNames = new LinkedHashSet<>(columnMetaData.keySet());
                ArrayList<Integer> typelist = new ArrayList<>();
                for (String column : columnNames) {
                    typelist.add(columnMetaData.get(column));
                }

                int[] typeArr = typelist.stream().mapToInt(Integer::intValue).toArray();
                String query = "SELECT * FROM " + sourceTable.getTableName();
                log.info("Executing SQL Statement [" + query + "]");
                ResultSet rs = stmt.executeQuery(query);


                int fetchLimit = 0;
                int rowCount = 0;

                List<Object[]> valueList = new ArrayList<>();
                System.out.println("Total Record Count: " + recordCount);
                try (ProgressBar pb = new ProgressBarBuilder().setInitialMax(recordCount).setStyle(ProgressBarStyle.ASCII).setTaskName("Migrating" + sourceTable.getTableName()).build()) {
                    while (rs.next()) {
                        ArrayList<Object> colValList = new ArrayList<>();
                        for (String column : columnNames) {
                            colValList.add(rs.getObject(column));
                            valueList.add(colValList.toArray());
                            fetchLimit++;
                            pb.step();
                            if (fetchLimit == recordCount || fetchLimit == BATCH_SIZE) {
                                try {
                                    long insertStart = System.currentTimeMillis();
                                    int insertCount = destinationTable.insertData(columnNames, valueList, typeArr);
                                    long insertEnd = System.currentTimeMillis();
                                    log.debug("Insert of " + BATCH_SIZE + " took " + (insertEnd - insertStart) + " MS ");
                                    rowCount += insertCount;
                                    recordCount -= insertCount;
                                } catch (RuntimeException e) {
                                    log.error(e.getMessage() + "\n" + Throwables.getStackTraceAsString(e));
                                    throw new RuntimeException("Migration Failed");
                                }
                                valueList.clear();
                                fetchLimit = 0;
                            }
                        }
                    }
                    rs.close();
                    stmt.close();
                    srcDatasrc.evictConnection(srcConn);
                    long endTime = System.currentTimeMillis();
                    log.info("PROCESSED " + rowCount + " RECORDS IN " + (endTime - startTime) + " MS");
                    System.out.println("PROCESSED " + rowCount + " RECORDS IN " + (endTime - startTime) + " MS");
                    return true;
                }

            } catch (SQLException e) {
                log.error(e.getMessage() + "\n" + Throwables.getStackTraceAsString(e));
                srcDatasrc.evictConnection(srcConn);
                throw new RuntimeException("Migration Failed");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            log.debug(e);
            log.debug(Throwables.getStackTraceAsString(e));
            throw new RuntimeException("Migration Failed");
        }
    }

    /**
     * Migrates data in sequential querying and inserts. Entire migration will be done in a single transaction. Make sure the transaction log file is large enough at destination DB
     *
     * @return status of migration (true SUCCESS,false FAILURE)
     */
    public boolean migrateDataMT() {
        try {
            try {
                sourceTable.verify();
                destinationTable.verify();
            } catch (IllegalArgumentException e) {
                log.error(e.getMessage() + "In" + Throwables.getStackTraceAsString(e));
                return false;
            } catch (RuntimeException e) {
                log.error(e.getMessage());
                return false;
            }

            long recordCount = sourceTable.getRecordCount();
            if (recordCount > WARN_THRESHOLD) {
                try {
                    warnUser(recordCount);
                } catch (IllegalArgumentException e) {
                    log.error(e.toString());
                    return false;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    log.info(e.toString());
                    return false;
                }
            }
            if (appendToDestination == false) {
                destinationTable.clearRecords();
            }
            long startTime = System.currentTimeMillis();
            HikariDataSource srcDatasrc = (HikariDataSource) sourceTable.getDataSource();
            Connection srcConn = null;

            try {
                try {
                    srcConn = srcDatasrc.getConnection();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    log.error(Throwables.getStackTraceAsString(e));
                    throw new RuntimeException("Error in fetching connection to source DB");
                }

                Statement stmt = srcConn.createStatement();
                stmt.setFetchSize(BATCH_SIZE);
                Map<String, Integer> columnMetaData = destinationTable.getColumnMetaData();
                LinkedHashSet<String> columnNames = new LinkedHashSet<>(columnMetaData.keySet());
                ArrayList<Integer> typelist = new ArrayList<>();
                for (String column : columnNames) {
                    typelist.add(columnMetaData.get(column));
                }

                int[] typeArr = typelist.stream().mapToInt(Integer::intValue).toArray();

                ExecutorService executorService = Executors.newFixedThreadPool(destinationTable.getPoolSize());
                List<Future<Integer>> futureList = new ArrayList<>();

                String query = "SELECT * FROM " + sourceTable.getTableName();
                log.info("Executing SQL Statement [" + query + "]");
                ResultSet rs = stmt.executeQuery(query);


                int fetchLimit = 0;
                int rowCount = 0;
                int insertID = 1;

                List<Object[]> valueList = new ArrayList<>();
                System.out.println("Total Record Count: " + recordCount);
                try (ProgressBar pb = new ProgressBarBuilder().setInitialMax(recordCount).setStyle(ProgressBarStyle.ASCII).setTaskName("Migrating" + sourceTable.getTableName()).build()) {
                    while (rs.next()) {
                        ArrayList<Object> colValList = new ArrayList<>();
                        for (String column : columnNames) {
                            colValList.add(rs.getObject(column));
                            valueList.add(colValList.toArray());
                            fetchLimit++;
                            pb.step();
                            if (fetchLimit == recordCount || fetchLimit == BATCH_SIZE) {
                                try {
                                    long insertStart = System.currentTimeMillis();
                                    futureList.add(executorService.submit(new InsertService(valueList, insertID)));
                                    long insertEnd = System.currentTimeMillis();
                                    log.debug("Insert of " + BATCH_SIZE + " took " + (insertEnd - insertStart) + " MS ");
                                    pb.stepBy(fetchLimit);
                                    recordCount -= fetchLimit;
                                } catch (RuntimeException e) {
                                    log.error(e.getMessage() + "\n" + Throwables.getStackTraceAsString(e));
                                    throw new RuntimeException("Migration Failed");
                                }
                                valueList.clear();
                                fetchLimit = 0;
                            }
                        }
                    }
                    rs.close();
                    stmt.close();
                    srcDatasrc.evictConnection(srcConn);
                    System.out.println("Completing insertion");
                    shutdownAndAwaitTermination(executorService);
                    List<Integer> results = futureList.stream().map(integerFuture -> {
                        try {
                            return integerFuture.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());

                    for (int result : results) {
                        rowCount += result;
                    }
                    long endTime = System.currentTimeMillis();
                    log.info("PROCESSED " + rowCount + " RECORDS IN " + (endTime - startTime) + " MS");
                    System.out.println("PROCESSED " + rowCount + " RECORDS IN " + (endTime - startTime) + " MS");
                    return true;
                }

            } catch (SQLException e) {
                log.error(e.getMessage() + "\n" + Throwables.getStackTraceAsString(e));
                srcDatasrc.evictConnection(srcConn);
                throw new RuntimeException("Migration Failed");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            log.debug(e);
            log.debug(Throwables.getStackTraceAsString(e));
            throw new RuntimeException("Migration Failed");
        }
    }

    /**
     * Warns the user if record count to be migrated is greater than WARN_THRESHOLD
     *
     * @param recordCount No. of records to be migrated
     * @throws RuntimeException
     * @throws IllegalArgumentException
     */
    public void warnUser(long recordCount) throws RuntimeException, IllegalArgumentException {
        System.out.println("Record Count is greater than 1 Million [" + recordCount + "Rows]. Migration of large tables might consume more memory. Proceed with CAUTION!!!");
        System.out.print("Do you want to proceed [y|n]? ");
        Scanner sc = new Scanner(System.in);
        String choice = sc.nextLine();
        if (Character.toLowerCase(choice.charAt(0)) == 'y') {
            System.out.println("Proceeding...");
            log.info("Record Count is greater than 1 Million [" + recordCount + "Rows]. Migration of large tables might consume more memory. User chose to proceed");
        } else if (Character.toLowerCase(choice.charAt(0)) == 'n') {
            log.info("Record Count is greater than 1 Million. Migration of large tables might consume more memory. User chose to cancel");
            throw new RuntimeException("Migration Canceled by User");
        } else {
            System.out.println("Invalid input");
            log.info("Record Count is greater than 1 Million [" + recordCount + " Rows]. Migration of large tables might consume more memory. User provided invalid input");
            throw new IllegalArgumentException("Invalid option. Migration cancelled");
        }
    }


    /**
     * Helper method which handles the shutdown of threads
     *
     * @param pool Thread Pool
     */
    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                while (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    Thread.sleep(60000);
                }
            }
        } catch (InterruptedException ex) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Helper inner class which represents the task for multithreaded insert operation. Each thread holds a Ins
     */
    public class InsertService implements Callable<Integer> {
        /**
         * Insert Row values
         */
        public final List<Object[]> valueList;
        /**
         * Id
         */
        public final int id;

        /**
         * Constructs an object of InsertService
         *
         * @param valueList Row values to be inserted of the table
         * @param id        Batch identifier
         */
        public InsertService(List<Object[]> valueList, int id) {
            this.valueList = new ArrayList<>(valueList);
            this.id = id;
        }

        @Override
        public Integer call() throws Exception {
            try {
                long batchStartTime = System.currentTimeMillis();
                int insertCount = destinationTable.insertData(columnNames, valueList, typeArr);
                long batchEndTime = System.currentTimeMillis();
                log.info(" BATCH " + id + " PROCESSED " + insertCount + " ROWS IN " + (batchEndTime - batchStartTime) + " MS");
                valueList.clear();
                return insertCount;
            } catch (Exception e) {
                log.error("Failure in Batch " + id);
                log.error(e.getMessage());
                log.error(Throwables.getStackTraceAsString(e));
                throw new RuntimeException("Failure in Batch " + id);
            }

        }
    }

    /**
     * Displays Queried data.
     *
     * @param columnNames column Nsmrd
     * @param queryResult Queried data
     * @see <a href="https://code.google.com/archive/p/j-text-utils/">j-text-utils</a>
     */
    public void display(LinkedHashSet<String> columnNames, List<Object[]> queryResult) {
        if (queryResult.size() == 0) {
            System.out.println("No records found!!");
        }
        String[] colNames = new String[columnNames.size()];
        int i = 0;
        for (String col : columnNames) {
            colNames[i] = col;
            i++;
        }
        Object[][] data = new Object[queryResult.size()][];
        i = 0;
        for (Object[] row : queryResult) {
            data[i] = row;
            i++;
        }
        TextTable table = new TextTable(colNames, data);
        table.printTable();
    }
}