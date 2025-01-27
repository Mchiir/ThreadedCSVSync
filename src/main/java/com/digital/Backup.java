package com.digital;

import com.github.javafaker.Faker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Backup {
    // Total time taken: 707 seconds, thus 11.7 min

    private static final String DB_URL = "jdbc:mysql://localhost:3306/db04";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    private static final int TOTAL_RECORDS = 10000000; // Total records to insert
    private static final int THREAD_COUNT = 20;         // Number of threads
    private static final int BATCH_SIZE = 50000;        // Increased batch size for inserts

    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        setupDataSource(); // Initialize HikariCP connection pool

        // Record start time
        LocalTime startTime = LocalTime.now();
        System.out.println("Data insertion started at: " + startTime);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        int recordsPerThread = TOTAL_RECORDS / THREAD_COUNT;

        for (int i = 0; i < THREAD_COUNT; i++) {
            int start = i * recordsPerThread + 1;
            int end = (i == THREAD_COUNT - 1) ? TOTAL_RECORDS : start + recordsPerThread - 1;
            executorService.execute(new DataInserterTask(start, end));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all threads to complete
        }

        // Record end time
        LocalTime endTime = LocalTime.now();
        System.out.println("Data insertion completed at: " + endTime);

        // Calculate and print total time taken
        Duration duration = Duration.between(startTime, endTime);
        System.out.println("Total time taken: " + duration.getSeconds() + " seconds");

        // Close the connection pool
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private static void setupDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_USER);
        config.setPassword(DB_PASSWORD);
        config.setMaximumPoolSize(THREAD_COUNT * 2); // Adjust pool size for optimal performance
        config.setMinimumIdle(THREAD_COUNT / 2);
        config.setIdleTimeout(30000);
        config.setMaxLifetime(1800000);
        dataSource = new HikariDataSource(config);
    }

    static class DataInserterTask implements Runnable {
        private final int start;
        private final int end;

        DataInserterTask(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            Faker faker = new Faker();
            String sql = "INSERT INTO people2 (name, email, address, age) VALUES (?, ?, ?, ?)";

            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false); // Disable auto-commit for batch inserts
                try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int batchCounter = 0;

                    for (int i = start; i <= end; i++) {
                        preparedStatement.setString(1, faker.name().fullName());
                        preparedStatement.setString(2, faker.internet().emailAddress());
                        preparedStatement.setString(3, faker.address().fullAddress());
                        preparedStatement.setInt(4, faker.number().numberBetween(18, 99));
                        preparedStatement.addBatch();

                        batchCounter++;

                        // Execute batch when batch size is reached
                        if (batchCounter % BATCH_SIZE == 0) {
                            preparedStatement.executeBatch();
                            batchCounter = 0;
                        }
                    }

                    // Execute remaining records in batch
                    if (batchCounter > 0) {
                        preparedStatement.executeBatch();
                    }

                    connection.commit(); // Commit all inserts at once
                    System.out.println("Thread " + Thread.currentThread().getName() + " completed records: " + start + " to " + end);

                } catch (SQLException e) {
                    connection.rollback(); // Rollback on error
                    System.err.println("Error in thread " + Thread.currentThread().getName() + ": " + e.getMessage());
                }
            } catch (SQLException e) {
                System.err.println("Error getting connection for thread " + Thread.currentThread().getName() + ": " + e.getMessage());
            }
        }
    }
}