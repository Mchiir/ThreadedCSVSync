package com.digital;

import com.github.javafaker.Faker;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FakeDataToDB {
    public static void main(String[] args) {
        // Total time for Multi-threaded data insertion: 1111282 ms,
        // thus 18.5 min
        long startTime = System.currentTimeMillis();

        String jdbcUrl = "jdbc:mysql://localhost:3306/db04";
        String user = "root";
        String password = "";
        int totalRecords = 10_000_000; // Total records to insert
        int numThreads = 4; // Use 4 threads for multi-threading

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        int recordsPerThread = totalRecords / numThreads;

        for (int i = 0; i < numThreads; i++) {
            int startIdx = i * recordsPerThread;
            int endIdx = (i + 1) * recordsPerThread;

            // Submit each task for insertion
            executorService.submit(new InsertDataTask(jdbcUrl, user, password, startIdx, endIdx));
        }

        // Shut down the executor and wait for all tasks to finish
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total time for Multi-threaded data insertion: " + totalTime + " ms");
    }
}

class InsertDataTask implements Runnable {
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private int startIdx;
    private int endIdx;

    public InsertDataTask(String jdbcUrl, String jdbcUser, String jdbcPassword, int startIdx, int endIdx) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    @Override
    public void run() {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)) {
            connection.setAutoCommit(false); // Disable auto-commit for performance improvement
            Faker faker = new Faker();
            String insertSQL = "INSERT INTO people4 (name, email, address, age) VALUES (?, ?, ?, ?)";

            try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
                int batchCount = 0;

                for (int i = startIdx; i < endIdx; i++) {
                    String name = faker.name().fullName();
                    String email = faker.internet().emailAddress();
                    String address = faker.address().fullAddress();
                    int age = faker.number().numberBetween(18, 100);

                    statement.setString(1, name);
                    statement.setString(2, email);
                    statement.setString(3, address);
                    statement.setInt(4, age);

                    statement.addBatch(); // Add to batch

                    // Execute batch every 1000 records
                    if (++batchCount % 1000 == 0 || i == endIdx - 1) {
                        statement.executeBatch(); // Execute batch for every 1000 records or final batch
                    }
                }

                connection.commit(); // Commit after all records are inserted in the batch
            } catch (SQLException e) {
                connection.rollback(); // Rollback in case of an error
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}