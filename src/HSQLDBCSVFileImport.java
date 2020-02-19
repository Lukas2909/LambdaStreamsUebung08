import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HSQLDBCSVFileImport {
    private Connection connection;
    private String driverName = "jdbc:hsqldb:";
    private String username = "sa";
    private String password = "";

    public static void main(String... args) {
        HSQLDBCSVFileImport application = new HSQLDBCSVFileImport();
        application.init();
        application.importCSVFile(Configuration.instance.dataPath + "records.csv");
        application.shutdown();
    }

    public void startup() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            String databaseURL = driverName + Configuration.instance.dataPath + "records.db";
            connection = DriverManager.getConnection(databaseURL, username, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public synchronized void update(String sqlStatement) {
        try {
            Statement statement = connection.createStatement();
            int result = statement.executeUpdate(sqlStatement);
            if (result == -1)
                System.out.println("error executing " + sqlStatement);
            statement.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }

    public void dropTable() {
        System.out.println("--- dropTable");

        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("DROP TABLE data");
        System.out.println("sqlStringBuilder : " + sqlStringBuilder.toString());

        update(sqlStringBuilder.toString());
    }

    public void createTable() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE data ").append(" ( ");
        sqlStringBuilder.append("id BIGINT NOT NULL").append(",");
        sqlStringBuilder.append("city INTEGER NOT NULL").append(",");
        sqlStringBuilder.append("area VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("shift VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("victimType INTEGER NOT NULL").append(",");
        sqlStringBuilder.append("victimAge INTEGER NOT NULL").append(",");
        sqlStringBuilder.append("daysInHospital INTEGER NOT NULL").append(",");
        sqlStringBuilder.append("PRIMARY KEY (id)");
        sqlStringBuilder.append(" )");
        update(sqlStringBuilder.toString());
    }

    public void init() {
        startup();
        dropTable();
        createTable();
    }

    public String buildSQLStatement(long id, int city, String area, String shift, int victimType, int victimAge, int daysInHospital) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO data (id,city,area,shift,victimType,victimAge,daysInHospital) VALUES (");
        stringBuilder.append(id).append(",");
        stringBuilder.append(city).append(",");
        stringBuilder.append("'").append(area).append("'").append(",");
        stringBuilder.append("'").append(shift).append("'").append(",");
        stringBuilder.append(victimType).append(",");
        stringBuilder.append(victimAge).append(",");
        stringBuilder.append(daysInHospital);
        stringBuilder.append(")");
        //System.out.println(stringBuilder.toString());
        return stringBuilder.toString();
    }

    public void insert(long id, int city, String area, String shift, int victimType, int victimAge, int daysInHospital) {
        update(buildSQLStatement(id, city, area, shift, victimType, victimAge, daysInHospital));
    }

    public void importCSVFile(String fileName) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                insert(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]), strings[2], strings[3], Integer.parseInt(strings[4]),
                        Integer.parseInt(strings[5]), Integer.parseInt(strings[6]));
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    public void shutdown() {
        try {
            Statement statement = connection.createStatement();
            statement.execute("SHUTDOWN");
            connection.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }
}