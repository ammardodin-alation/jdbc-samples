import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLServerSample {
  static final List<String> queries = new ArrayList<>() {{
//    add("CREATE TABLE visits(value INT);");
//    add("INSERT INTO visits(value) VALUES (1), (2), (3);");
    add(
            "SELECT * FROM visits;" + // This returns a result
            "SELECT * FROM table_does_not_exist;" + // This errors
            "SELECT * FROM visits;" + // This does not execute
            "SELECT * FROM visits;"); // This does not execute
  }};
  static final Logger LOGGER = Logger.getLogger(SQLServerSample.class.getName());

  public static void main(String[] args) throws Throwable {
    LOGGER.setUseParentHandlers(false);
    ConsoleHandler consoleHandler = new ConsoleHandler();
    Formatter formatter = new CustomFormatter();
    consoleHandler.setFormatter(formatter);
    LOGGER.addHandler(consoleHandler);

    try {
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Failed to initialize JDBC driver.");
      e.printStackTrace();
      System.exit(1);
    }

    boolean hasResultSet;

    try (Connection connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=master;user=sa;password=Passw0rd;encrypt=false;")) {
      LOGGER.info("Successfully connected");
      LOGGER.info("Autocommit " + connection.getAutoCommit());
      try (Statement statement = connection.createStatement()) {
        for (String query : queries) {
          LOGGER.info("Processing Query: " + query);
          hasResultSet = statement.execute(query);
          ResultSetMetaData resultSetMetaData;
          LOGGER.info("HasInitialResultSet: " + hasResultSet);
          int counter = 1;
          while (true) {
            if (hasResultSet) {
              try (ResultSet resultSet = statement.getResultSet()) {
                resultSetMetaData = resultSet.getMetaData();
                int columnCount = resultSetMetaData.getColumnCount();
                for (int j = 1; j <= columnCount; ++j) {
                  LOGGER.info("Index: " + j + " | ColumnName: " + resultSetMetaData.getColumnName(j) + " | ColumnType: " + resultSetMetaData.getColumnTypeName(j));
                }
                StringBuilder stringBuilder = new StringBuilder();
                while (resultSet.next()) {
                  for (int k = 1; k <= columnCount; ++k) {
                    stringBuilder.append(resultSetMetaData.getColumnType(k));
                    stringBuilder.append('|');
                    stringBuilder.append(resultSetMetaData.getColumnTypeName(k));
                    stringBuilder.append('|');
                    stringBuilder.append(resultSetMetaData.getColumnClassName(k));
                    stringBuilder.append('|');
                    stringBuilder.append(resultSetMetaData.getColumnName(k));
                    stringBuilder.append('=');
                    stringBuilder.append(resultSet.getString(k));
                    stringBuilder.append('|');
                  }
                  LOGGER.info(stringBuilder.toString());
                  stringBuilder.setLength(0);
                }
              }
            } else {
              if (statement.getUpdateCount() == -1) {
                LOGGER.info("Exhausted all results");
                break;
              }
              LOGGER.info("Result " + counter + " is just a count: " + statement.getUpdateCount());
            }
            counter++;
            while (true) {
              LOGGER.info("Attempting to fetch more results");
              try {
                hasResultSet = statement.getMoreResults();
                break;
              } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Errored getting the next result set | Error = " + e .getMessage());
                LOGGER.log(Level.WARNING, "Attempting to skip and fetch next result set");
              }
            }
          }
        }
      }
    } catch (Exception exception) {
      exception.printStackTrace();
      LOGGER.warning(exception.getClass().getName() + ":" + exception.getMessage());
    }
  }
}
