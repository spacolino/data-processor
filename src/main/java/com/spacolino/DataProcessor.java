package com.spacolino;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class DataProcessor {
    private static final String DB_URL = "jdbc:h2:mem:datadb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE";
    private static final String USER = "sa";
    private static final String PASS = "";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Please provide the file path as a command-line argument.");
            return;
        }
        String filePath = args[0];

        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            conn.setAutoCommit(false);
            createTable(conn);
            processFileInBatches(conn, filePath);
            printDateInsertRange(conn);
            printSampleData(conn);
            checkDataInDB(conn);
            System.out.println("Press Enter to exit.");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS events " +
                "(id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                " match_id BIGINT, " +
                " market_id BIGINT, " +
                " outcome_id BIGINT, " +
                " specifiers VARCHAR(255), " +
                " date_insert TIMESTAMP)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            stmt.execute("CREATE INDEX idx_match_id ON events (match_id)");
        }
    }

    private static void processFileInBatches(Connection conn, String filePath) throws Exception {
        String sql = "INSERT INTO events (match_id, market_id, outcome_id, specifiers, date_insert) VALUES (?, ?, ?, ?, ?)";
        Map<Long, PriorityBlockingQueue<Event>> eventQueues = new ConcurrentHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line;
            int batchSize = 1000;
            long lineCount = 0;
            int sequenceNumber = 0;

            // Skip the header line
            br.readLine();

            while ((line = br.readLine()) != null) {

                // Remove BOM if present
                if (lineCount == 0 && line.startsWith("\uFEFF")) {
                    line = line.substring(1);
                }

                String[] data = line.split("\\|");
                if (data.length < 4) {
                    System.out.println("Skipping invalid line: " + line);
                    continue;
                }

                try {
                    long matchId = Long.parseLong(data[0]);
                    Event event = new Event(
                            matchId,
                            Long.parseLong(data[1]),
                            Long.parseLong(data[2]),
                            data[3],
                            LocalDateTime.now(),
                            sequenceNumber++
                    );
                    eventQueues.computeIfAbsent(matchId, k -> new PriorityBlockingQueue<>()).add(event);

                    if (lineCount % batchSize == 0 && lineCount > 0) {
                        processBatch(conn, pstmt, eventQueues);
                    }

                    lineCount++;
                    if (lineCount % 10000 == 0) {
                        System.out.println("Processed " + lineCount + " lines");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Skipping line with invalid number format: " + line);
                }
            }

            // Process any remaining events
            processBatch(conn, pstmt, eventQueues);

            System.out.println("Total lines processed: " + lineCount);
        }
    }

    private static void processBatch(Connection conn, PreparedStatement pstmt, Map<Long, PriorityBlockingQueue<Event>> eventQueues) throws SQLException {
        for (PriorityBlockingQueue<Event> queue : eventQueues.values()) {
            while (!queue.isEmpty()) {
                Event event = queue.poll();
                pstmt.setLong(1, event.getMatchId());
                pstmt.setLong(2, event.getMarketId());
                pstmt.setLong(3, event.getOutcomeId());
                pstmt.setString(4, event.getSpecifiers());
                pstmt.setTimestamp(5, Timestamp.valueOf(event.getDateInsert()));
                pstmt.addBatch();
            }
        }
        pstmt.executeBatch();
        conn.commit();
    }

    private static void printDateInsertRange(Connection conn) throws SQLException {
        String sql = "SELECT MIN(date_insert), MAX(date_insert) FROM events";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                System.out.println("Min date_insert: " + rs.getTimestamp(1));
                System.out.println("Max date_insert: " + rs.getTimestamp(2));
            }
        }
    }

    private static void printSampleData(Connection conn) throws SQLException {
        String sql = "SELECT * FROM events LIMIT 5";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("\nSample data:");
            while (rs.next()) {
                System.out.printf("Match ID: %d, Market ID: %d, Outcome ID: %d, Specifiers: %s, Date Insert: %s%n",
                        rs.getLong("match_id"),
                        rs.getLong("market_id"),
                        rs.getLong("outcome_id"),
                        rs.getString("specifiers"),
                        rs.getTimestamp("date_insert")
                );
            }
        }
    }

    private static void checkDataInDB(Connection conn) throws SQLException {
        String sql = "SELECT * FROM events ORDER BY match_id, id LIMIT 20";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("\nSample data from database:");
            System.out.println("ID | Match ID | Market ID | Outcome ID | Specifiers | Date Insert");
            System.out.println("-------------------------------------------------------------------");
            while (rs.next()) {
                System.out.printf("%d | %d | %d | %d | %s | %s%n",
                        rs.getLong("id"),
                        rs.getLong("match_id"),
                        rs.getLong("market_id"),
                        rs.getLong("outcome_id"),
                        rs.getString("specifiers"),
                        rs.getTimestamp("date_insert")
                );
            }
        }

        String countSql = "SELECT COUNT(*) as total FROM events";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
                System.out.println("\nTotal number of records: " + rs.getLong("total"));
            }
        }
    }

    static class Event implements Comparable<Event> {
        private final long matchId, marketId, outcomeId;
        private final String specifiers;
        private final LocalDateTime dateInsert;
        private final int sequenceNumber;

        public Event(long matchId, long marketId, long outcomeId, String specifiers, LocalDateTime dateInsert, int sequenceNumber) {
            this.matchId = matchId;
            this.marketId = marketId;
            this.outcomeId = outcomeId;
            this.specifiers = specifiers;
            this.dateInsert = dateInsert;
            this.sequenceNumber = sequenceNumber;
        }

        public long getMatchId() { return matchId; }
        public long getMarketId() { return marketId; }
        public long getOutcomeId() { return outcomeId; }
        public String getSpecifiers() { return specifiers; }
        public LocalDateTime getDateInsert() { return dateInsert; }

        @Override
        public int compareTo(Event other) {
            return Integer.compare(this.sequenceNumber, other.sequenceNumber);
        }
    }
}