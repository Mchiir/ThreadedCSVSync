package com.digital;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DBDataToCSV {
    public static void main(String[] args) {
//        Total time for Multi-threaded reading and writing: 75845 ms,
        // thus 1.2640 minutes
        Long startTime = System.currentTimeMillis();

        String jdbcUrl = "jdbc:mysql://localhost:3306/db03";
        String user = "root";
        String password = "";
        String csvFilePath = "people2.csv"; // Output CSV file

        int totalRecords = 10_000_000; // Total records to fetch from DB
        int numThreads = 4; // Use 4 threads for multi-threading

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        int recordsPerThread = totalRecords / numThreads;

        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFilePath))) {
            // Write header for the CSV file
            csvWriter.writeNext(new String[]{"id", "name", "email", "address", "age"});

            for (int i = 0; i < numThreads; i++) {
                int startIdx = i * recordsPerThread;
                int endIdx = (i + 1) * recordsPerThread;

                // Submit each task
                executorService.submit(new ReadAndWriteTask(jdbcUrl, user, password, startIdx, endIdx, csvFilePath));
            }

            // Shut down the executor and wait for all tasks to finish
            executorService.shutdown();
            if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                executorService.shutdownNow();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total time for Multi-threaded reading and writing: " + totalTime + " ms");
    }
}

class ReadAndWriteTask implements Runnable {
    private String jdbcUrl;
    private String jdbcUser;
    private String jdbcPassword;
    private int startIdx;
    private int endIdx;
    private String csvFilePath;

    public ReadAndWriteTask(String jdbcUrl, String jdbcUser, String jdbcPassword, int startIdx, int endIdx, String csvFilePath) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.csvFilePath = csvFilePath;
    }

    @Override
    public void run() {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
             CSVWriter csvWriter = new CSVWriter(new FileWriter(csvFilePath, true))) { // Append mode

            String query = "SELECT id, name, email, address, age FROM people2 LIMIT ?, ?";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setInt(1, startIdx);
                statement.setInt(2, endIdx - startIdx); // Adjust the number of records to fetch

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        int id = resultSet.getInt("id");
                        String name = resultSet.getString("name");
                        String email = resultSet.getString("email");
                        String address = resultSet.getString("address");
                        int age = resultSet.getInt("age");

                        String[] record = {String.valueOf(id), name, email, address, String.valueOf(age)};
                        synchronized (csvWriter) { // Synchronize access to csvWriter
                            csvWriter.writeNext(record);
                        }
                    }
                }
            }

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}