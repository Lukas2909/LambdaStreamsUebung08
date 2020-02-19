import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Query implements IQuery {
    private Connection connection;
    private String driverName = "jdbc:hsqldb:";
    private String username = "sa";
    private String password = "";
    private BufferedWriter bufferedWriter;

    public static void main(String... args) {
        Query query = new Query();
        query.startup();

        query.executeSQL01();
        query.executeSQL02();
        query.executeSQL03();
        query.executeSQL04();
        query.executeSQL05();
        query.executeSQL06();
        query.executeSQL07();
        query.executeSQL08();
        query.executeSQL09();
        query.executeSQL10();
        query.executeSQL11();
        query.executeSQL12();
        query.executeSQL13();
        query.executeSQL14();

        query.shutdown();
    }

    public void startup() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            String databaseURL = driverName + Configuration.instance.dataPath + "records.db";
            connection = DriverManager.getConnection(databaseURL, username, password);

            bufferedWriter = new BufferedWriter(new FileWriter(Configuration.instance.logPath + "query.log"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeLogfile(String message) {
        try {
            bufferedWriter.append(message).append("\n");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    public String dump(ResultSet resultSet) {
        StringBuilder stringBuilder = new StringBuilder();

        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int maximumNumberColumns = resultSetMetaData.getColumnCount();
            int i;
            Object object;

            for (; resultSet.next(); ) {
                for (i = 0; i < maximumNumberColumns; ++i) {
                    object = resultSet.getObject(i + 1);
                    stringBuilder.append(object.toString() + " ");
                }
                stringBuilder.append(" \n");
            }

            return stringBuilder.toString();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }

        return "-1";
    }

    public synchronized void queryDump(String sqlStatement) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlStatement);
            writeLogfile(sqlStatement);
            writeLogfile(dump(resultSet));
            statement.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }

    // count
    public void executeSQL01() {
        writeLogfile("--- query 01 (count)");
        String sqlStatement = "SELECT COUNT(*) FROM data";
        queryDump(sqlStatement);
    }

    // count, where
    public void executeSQL02() {
        writeLogfile("--- query 02 (count, where)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE city = 40 AND area = 'b' AND victimType >= 2";
        queryDump(sqlStatement);
    }

    // count, where, in
    public void executeSQL03() {
        writeLogfile("--- query 03 (count, where, in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE city = 40 AND area IN ('a','c') AND victimType = 3 AND victimAge <= 18";
        queryDump(sqlStatement);
    }

    // count, where, not in
    public void executeSQL04() {
        writeLogfile("--- query 04 (count, where, not in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE city NOT IN (10,20,30,50) AND victimType = 1 AND victimAge >= 5 AND victimAge <= 25";
        queryDump(sqlStatement);
    }

    // sum, where, in
    public void executeSQL05() {
        writeLogfile("--- query 05 (sum, where, in)");
        String sqlStatement = "SELECT SUM(daysInHospital) FROM data " +
                "WHERE city = 10 AND area IN ('a','b') AND victimAge >= 20 AND victimAge <= 25";
        queryDump(sqlStatement);
    }

    // avg, where, not in
    public void executeSQL06() {
        writeLogfile("--- query 06 (avg, where, not in)");
        String sqlStatement = "SELECT AVG(daysInHospital) FROM data " +
                "WHERE city NOT IN (10,20,30,50) AND victimType = 1 AND victimAge >= 5 AND victimAge <= 25";
        queryDump(sqlStatement);
    }

    // id, where, in, order by desc limit
    public void executeSQL07() {
        writeLogfile("--- query 07 (id, where, in, order by desc limit)");
        String sqlStatement = "SELECT id FROM data " +
                "WHERE city = 40 AND area = 'b' AND victimType IN (1,3) AND victimAge = 18 AND daysInHospital >= 10 " +
                "ORDER BY daysInHospital DESC LIMIT 3";
        queryDump(sqlStatement);
    }

    // id, where, in, order by desc, order by asc
    public void executeSQL08() {
        writeLogfile("--- query 08 (id, where, in, order by desc, order by asc)");
        String sqlStatement = "SELECT id FROM data " +
                "WHERE city = 10 AND area IN ('a','b') AND victimType = 3 AND victimAge = 18 AND daysInHospital >= 10 " +
                "ORDER BY daysInHospital DESC, area";
        queryDump(sqlStatement);
    }

    // count, group by
    public void executeSQL09() {
        writeLogfile("--- query 09 (count, group by)");
        String sqlStatement = "SELECT shift,COUNT(*) FROM data " +
                "GROUP BY shift";
        queryDump(sqlStatement);
    }

    // count, where, group by
    public void executeSQL10() {
        writeLogfile("--- query 10 (count, where, group by)");
        String sqlStatement = "SELECT area,COUNT(*) FROM data " +
                "WHERE victimType = 2 AND victimAge = 18 " +
                "GROUP BY area";
        queryDump(sqlStatement);
    }

    // count, where, in, group by
    public void executeSQL11() {
        writeLogfile("--- query 11 (count, where, in, group by)");
        String sqlStatement = "SELECT victimType,COUNT(*) FROM data " +
                "WHERE city IN (10,20,30,40,50) AND area = 'c' AND victimAge >= 60 " +
                "GROUP BY victimType";
        queryDump(sqlStatement);
    }

    // count, where, not in, group by
    public void executeSQL12() {
        writeLogfile("--- query 12 (count, where, not in, group by)");
        String sqlStatement = "SELECT area,COUNT(*) FROM data " +
                "WHERE city NOT IN (10,20,30,40,50) AND shift = 'n' AND victimAge >= 30 AND victimAge <= 40 " +
                "GROUP BY area";
        queryDump(sqlStatement);
    }

    // sum, where, not in, in, group by
    public void executeSQL13() {
        writeLogfile("--- query 13 (sum, where, not in, in, group by)");
        String sqlStatement = "SELECT victimType,SUM(daysInHospital) FROM data " +
                "WHERE CITY NOT IN (10,20) AND area IN ('b','c') AND shift = 'd' AND victimAge >= 18 " +
                "GROUP BY victimType";
        queryDump(sqlStatement);
    }

    // avg, where, in, in, group by
    public void executeSQL14() {
        writeLogfile("--- query 14 (avg, where, in, in, group by)");
        String sqlStatement = "SELECT victimType,AVG(victimAge) FROM data " +
                "WHERE CITY IN (40,50) AND area IN ('a','c') AND shift = 'n' " +
                "GROUP BY victimType";
        queryDump(sqlStatement);
    }

    public void shutdown() {
        try {
            Statement statement = connection.createStatement();
            statement.execute("SHUTDOWN");
            connection.close();
            bufferedWriter.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}