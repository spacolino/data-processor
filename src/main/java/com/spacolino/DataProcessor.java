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
            System.out.println("Provide the file path as a command-line argument.");
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
                " outcome_id VARCHAR(255), " +
                " specifiers VARCHAR(255), " +
                " date_insert TIMESTAMP)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_match_id ON events (match_id)");
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
            int skippedLines = 0;

            // Skip the header line
            br.readLine();

            while ((line = br.readLine()) != null) {
                lineCount++;

                String[] data = line.split("\\|", -1);

                if (data.length < 4) {
                    System.out.println("Skipping invalid line " + lineCount + ": Insufficient fields");
                    skippedLines++;
                    continue;
                }

                try {
                    long matchId = parseId(data[0]);
                    long marketId = Long.parseLong(data[1].trim());
                    String outcomeId = parseOutcomeId(data[2]);
                    String specifiers = data[3].trim();

                    Event event = new Event(
                            matchId,
                            marketId,
                            outcomeId,
                            specifiers,
                            LocalDateTime.now(),
                            sequenceNumber++
                    );
                    eventQueues.computeIfAbsent(matchId, k -> new PriorityBlockingQueue<>()).add(event);

                    if (lineCount % batchSize == 0) {
                        processBatch(conn, pstmt, eventQueues);
                    }

                    if (lineCount % 10000 == 0) {
                        System.out.println("Processed " + lineCount + " lines");
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println("Skipping line " + lineCount + " with invalid format: " + line);
                    skippedLines++;
                }
            }

            // Process any remaining events
            processBatch(conn, pstmt, eventQueues);

            System.out.println("Total lines processed: " + lineCount);
            System.out.println("Total lines skipped: " + skippedLines);
        }
    }

    private static long parseId(String idString) {
        // if present remove single quotes
        idString = idString.replace("'", "");
        int lastColonIndex = idString.lastIndexOf(':');
        if (lastColonIndex != -1 && lastColonIndex < idString.length() - 1) {
            String numericPart = idString.substring(lastColonIndex + 1);
            return Long.parseLong(numericPart);
        }
        throw new IllegalArgumentException("Invalid ID format: " + idString);
    }

    private static String parseOutcomeId(String outcomeIdString) {
        // Remove single quotes if present
        return outcomeIdString.replace("'", "").trim();
    }



    private static void processBatch(Connection conn, PreparedStatement pstmt, Map<Long, PriorityBlockingQueue<Event>> eventQueues) throws SQLException {
        int insertedCount = 0;
        for (PriorityBlockingQueue<Event> queue : eventQueues.values()) {
            while (!queue.isEmpty()) {
                Event event = queue.poll();
                pstmt.setLong(1, event.getMatchId());
                pstmt.setLong(2, event.getMarketId());
                pstmt.setString(3, event.getOutcomeId());
                pstmt.setString(4, event.getSpecifiers());
                pstmt.setTimestamp(5, Timestamp.valueOf(event.getDateInsert()));
                pstmt.addBatch();
                insertedCount++;
            }
        }
        int[] updateCounts = pstmt.executeBatch();
        conn.commit();
        System.out.println("Inserted " + insertedCount + " events in this batch");
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
            System.out.println("ID | Match ID | Market ID | Outcome ID | Specifiers | Date Insert");
            System.out.println("-------------------------------------------------------------------");
            while (rs.next()) {
                System.out.printf("%d | %d | %d | %s | %s | %s%n",
                        rs.getLong("id"),
                        rs.getLong("match_id"),
                        rs.getLong("market_id"),
                        rs.getString("outcome_id"),  // Changed from getLong to getString
                        rs.getString("specifiers"),
                        rs.getTimestamp("date_insert")
                );
            }
        }
    }

    private static void checkDataInDB(Connection conn) throws SQLException {
        // Display sample data
        String sampleSql = "SELECT * FROM events ORDER BY match_id, id LIMIT 20";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleSql)) {
            System.out.println("\nSample data from database (first 20 rows):");
            System.out.println("ID | Match ID | Market ID | Outcome ID | Specifiers | Date Insert");
            System.out.println("-------------------------------------------------------------------");
            while (rs.next()) {
                System.out.printf("%d | %d | %d | %s | %s | %s%n",
                        rs.getLong("id"),
                        rs.getLong("match_id"),
                        rs.getLong("market_id"),
                        rs.getString("outcome_id"),  // Changed from getLong to getString
                        rs.getString("specifiers"),
                        rs.getTimestamp("date_insert")
                );
            }
        }

        // Count total records
        String countSql = "SELECT COUNT(*) as total FROM events";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
                System.out.println("\nTotal number of records: " + rs.getLong("total"));
            }
        }

        // Get statistics
        String statsSql = "SELECT COUNT(DISTINCT match_id) as unique_matches, " +
                "MIN(date_insert) as min_date, MAX(date_insert) as max_date " +
                "FROM events";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(statsSql)) {
            if (rs.next()) {
                System.out.println("Number of unique matches: " + rs.getLong("unique_matches"));
                System.out.println("Earliest insert date: " + rs.getTimestamp("min_date"));
                System.out.println("Latest insert date: " + rs.getTimestamp("max_date"));
            }
        }

        // Get top 5 matches by number of events
        String topMatchesSql = "SELECT match_id, COUNT(*) as event_count " +
                "FROM events GROUP BY match_id ORDER BY event_count DESC LIMIT 5";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(topMatchesSql)) {
            System.out.println("\nTop 5 matches by number of events:");
            System.out.println("Match ID | Event Count");
            System.out.println("----------------------");
            while (rs.next()) {
                System.out.printf("%d | %d%n", rs.getLong("match_id"), rs.getLong("event_count"));
            }
        }
    }

    static class Event implements Comparable<Event> {
        private final long matchId, marketId;
        private final String outcomeId;
        private final String specifiers;
        private final LocalDateTime dateInsert;
        private final int sequenceNumber;

        public Event(long matchId, long marketId, String outcomeId, String specifiers, LocalDateTime dateInsert, int sequenceNumber) {
            this.matchId = matchId;
            this.marketId = marketId;
            this.outcomeId = outcomeId;
            this.specifiers = specifiers;
            this.dateInsert = dateInsert;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public int compareTo(Event other) {
            return Integer.compare(this.sequenceNumber, other.sequenceNumber);
        }

        public long getMatchId() {
            return matchId;
        }

        public long getMarketId() {
            return marketId;
        }

        public String getOutcomeId() {
            return outcomeId;
        }

        public String getSpecifiers() {
            return specifiers;
        }

        public LocalDateTime getDateInsert() {
            return dateInsert;
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }
    }
}