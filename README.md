## Prerequisites

- Java JDK 21
- Maven 3.6 or later

## Running the Application

1. Build the project:
   ```
   mvn clean install
   ```

2. Run the application:
   ```
   mvn exec:java -Dexec.mainClass="com.spacolino.DataProcessor" -Dexec.args="/path/to/your/data.txt"
   ```
   Replace `/path/to/your/data.txt` with the path to your input file.

## Input Data Format

The input file should be pipe-separated (|) with columns:
MATCH_ID | MARKET_ID | OUTCOME_ID | SPECIFIERS

Example:
```
'sr:match:12345678'|60|'2'|
'sr:match:87654321'|218|'5'|'setnr=2|gamenr=6|pointnr=1'
```

## Output

The app will print:
- Processing progress
- Number of records inserted
- Sample data from the database
- Basic statistics about the processed data

## Notes

- The application uses an in-memory H2 database.
- Data is processed in batches for efficiency.
- OUTCOME_ID is stored as VARCHAR to consume different formats.