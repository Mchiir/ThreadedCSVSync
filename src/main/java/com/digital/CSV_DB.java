package com.digital;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@code CSV_DB} class reads data from a CSV file and inserts it into
 * the "people3" table in a MySQL database using multithreading and batch insertion.
 *
 * <p>
 * <strong>Key Features:</strong>
 * </p>
 * <ul>
 *     <li>Reads data from "people2.csv"</li>
 *     <li>Uses multithreading to divide workload</li>
 *     <li>Batch processing for efficient insertion</li>
 *     <li>Progress monitoring</li>
 * </ul>
 */
public class CSV_DB {
    // Data insertion completed. Total Records Inserted: 10_000_000
    // Time taken: 5 minutes and 43 seconds.

    private static final String DB_URL = "jdbc:mysql://localhost:3306/db03"; // change DB name, here I used 'db03'
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "user";

    private static final String CSV_FILE = "people2.csv";
    private static final int THREAD_COUNT = 10; // Number of threads
    private static final int BATCH_SIZE = 1000; // Batch size for insertion

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE))) {
            // Skip header line
            String header = reader.readLine();
            if (header == null || header.isEmpty()) {
                System.out.println("The CSV file is empty or missing the header row.");
                return;
            }

            // Count total rows in the CSV file (excluding the header)
            long totalRecords = reader.lines().count();
            System.out.println("Total Records: " + totalRecords);

            AtomicInteger totalRecordsInserted = new AtomicInteger(0);
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

            // Re-open reader to process data (reset to beginning)
            reader.close();
            BufferedReader dataReader = new BufferedReader(new FileReader(CSV_FILE));
            dataReader.readLine(); // Skip header again

            // Split data into chunks and process with threads
            String[] rows = dataReader.lines().toArray(String[]::new);
            int recordsPerThread = (int) Math.ceil((double) rows.length / THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                int start = i * recordsPerThread;
                int end = Math.min(start + recordsPerThread, rows.length);
                String[] chunk = new String[end - start];
                System.arraycopy(rows, start, chunk, 0, end - start);

                executorService.submit(new DataInserter(chunk, totalRecordsInserted));
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                // Wait for all threads to complete
            }

            long endTime = System.currentTimeMillis();
            long elapsedTime = (endTime - startTime) / 1000; // Time in seconds
            System.out.println("Data insertion completed. Total Records Inserted: " + totalRecordsInserted.get());
            System.out.println("Time taken: " + elapsedTime / 60 + " minutes and " + elapsedTime % 60 + " seconds.");

        } catch (Exception e) {
            e.printStackTrace();
        }
}

static class DataInserter implements Runnable {
        private final String[] rows;
        private final AtomicInteger totalRecordsInserted;

        DataInserter(String[] rows, AtomicInteger totalRecordsInserted) {
            this.rows = rows;
            this.totalRecordsInserted = totalRecordsInserted;
        }

        @Override
        public void run() {
            // Updated SQL statement (id is auto-incremented in MySQL)
            String insertSQL = "INSERT INTO people3 (name, email, address, age) VALUES (?, ?, ?, ?)";

            try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                 PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

                connection.setAutoCommit(false); // Disable auto-commit for batch processing

                int batchCount = 0;
                for (String row : rows) {
                    String[] values = parseCsvRow(row);

                    // Set the values for name, email, address, and age
                    preparedStatement.setString(1, values[1]); // name
                    preparedStatement.setString(2, values[2]); // email
                    preparedStatement.setString(3, values[3]); // address

                    // Ensure age is parsed as an integer
                    try {
                        preparedStatement.setInt(4, Integer.parseInt(values[4])); // age
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid age value: " + values[3]);
                        continue; // Skip this record if the age is invalid
                    }
//                    System.out.println(preparedStatement);
                    preparedStatement.addBatch();
                    batchCount++;

                    if (batchCount % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        connection.commit();
                        batchCount = 0;
                    }

                    // Increment total inserted count
                    totalRecordsInserted.incrementAndGet();
                }

                preparedStatement.executeBatch(); // Execute remaining records
                connection.commit(); // Commit the final batch

                System.out.println("Thread " + Thread.currentThread().getName() + " completed processing its chunk.");

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String[] parseCsvRow(String row) {
            List<String> fields = new ArrayList<>();
            StringBuilder field = new StringBuilder();
            boolean inQuotes = false;

            for (char c : row.toCharArray()) {
                if (c == '"' && (field.length() == 0 || row.charAt(field.length() - 1) != '\\')) {
                    // Toggle the inQuotes flag when encountering an unescaped double quote
                    inQuotes = !inQuotes;
                } else if (c == ',' && !inQuotes) {
                    // If we're not inside quotes, treat comma as field separator
                    fields.add(field.toString().trim());
                    field = new StringBuilder();  // Reset the field for next value
                } else {
                    // Otherwise, append the character to the current field
                    field.append(c);
                }
            }

            // Add the last field
            fields.add(field.toString().trim());

            return fields.toArray(new String[0]);
        }
    }
}