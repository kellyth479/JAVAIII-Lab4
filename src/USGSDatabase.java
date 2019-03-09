import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.*;

public class USGSDatabase {

    public static void main(String[] args) {
        String connectionUrl = "jdbc:sqlserver://localhost:1433;databaseName=master;user=sa;password=yourStrong(!)Password";


        try (Connection conn = DriverManager.getConnection(connectionUrl)) {

//            runSql (conn, "drop TABLE earthquake_data");

            createTable(conn,"earthquake_data");

//            loadData(conn,"earthquake_data", "S:\\Dev\\JAVA III\\Lab4\\2007-2017_large_quake.csv");
            loadData(conn, "2007-2017_large_quake.csv");

            conn.close();


        } catch (Exception e) {
            System.out.println();
            e.printStackTrace();
        }
    }
    public static void createTable (Connection conn, String tableName ) {
        try {
            java.sql.Statement stmt = conn.createStatement();

            String createTableSql =" CREATE TABLE " + tableName +
                    "(time  VARCHAR(50), " +
                    " latitude DECIMAL (7,4), " +
                    " longitude DECIMAL (7,4), " +
                    " depth DECIMAL(6,2), " +
                    " mag DECIMAL(6,2), " +
                    " magType VARCHAR (50), " +
                    " nst INTEGER, " +
                    " gap INTEGER, " +
                    " dmin DECIMAL(6,4), " +
                    " rms DECIMAL (6,4), " +
                    " net VARCHAR (50), " +
                    " id VARCHAR(50) not NULL, " +
                    " updated VARCHAR(25), " +
                    " place VARCHAR (255), " +
                    " type VARCHAR(25), " +
                    " horizontalError INTEGER, " +
                    " depthError INTEGER, " +
                    " magError INTEGER, " +
                    " magNst INTEGER, " +
                    " status VARCHAR (25), " +
                    " locationSource VARCHAR (10), " +
                    " magSource VARCHAR (10), " +
                    " PRIMARY KEY ( id ))";



            stmt.executeUpdate(createTableSql);
            System.out.println(createTableSql + "table created");


        } catch (Exception e) {
            System.out.println();
            e.printStackTrace();
        }
    }

    public static void runSql(Connection conn, String sql) {


        try {

            System.out.println("Running SQL " + sql);
            java.sql.Statement stmt = conn.createStatement();

            stmt.executeUpdate(sql);

            System.out.println("done ..");

        } catch (Exception e) {
            System.out.println();
            e.printStackTrace();
        }

    }
    public static void loadData(Connection conn, String csvFile) throws SQLException {


        try {
            Reader reader = Files.newBufferedReader(Paths.get(csvFile));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withIgnoreSurroundingSpaces().withNullString("")
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
            //final Map<String, Integer> headerMap = csvParser.getHeaderMap();
            //final List<String> labels = new ArrayList<>(headerMap.size());
	        /*
	        for (final String label : headerMap.keySet()) {
	        	if(headerMap.get(label) <22) {
	            System.out.println(label + " " + headerMap.get(label) );

	        	}
	        }
	        */
            PreparedStatement preparedStatement = null;
            final StringBuffer insertTableSQL = new StringBuffer("INSERT INTO earthquake_data "
                    + "(time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place"
                    + ",type,horizontalError,depthError,magError,magNst,status,locationSource,magSource)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            preparedStatement=  conn.prepareStatement(insertTableSQL.toString());

            for (CSVRecord csvRecord : csvParser) {



                //final StringBuffer sql = new StringBuffer(sql1);
                preparedStatement.setString(1, csvRecord.get(0));
                preparedStatement.setString(2, csvRecord.get(1));
                preparedStatement.setString(3, csvRecord.get(2));
                if(csvRecord.get(3)!=null) {
                    preparedStatement.setFloat(4, Float.parseFloat(csvRecord.get(3)));
                }
                if(csvRecord.get(4)!=null) {
                    preparedStatement.setFloat(5, Float.parseFloat(csvRecord.get(4)));
                }
                preparedStatement.setString(6, csvRecord.get(5));
                if(csvRecord.get(6) !=null) {
                    preparedStatement.setFloat(7, Float.parseFloat(csvRecord.get(6)));
                }else {preparedStatement.setInt(7,0);}
                if(csvRecord.get(7) !=null) {
                    preparedStatement.setFloat(8, Float.parseFloat(csvRecord.get(7)));
                }
                if(csvRecord.get(8) !=null) {
                    preparedStatement.setFloat(9, Float.parseFloat(csvRecord.get(8)));
                }
                if(csvRecord.get(9) !=null) {
                    preparedStatement.setFloat(10, Float.parseFloat(csvRecord.get(9)));
                }
                preparedStatement.setString(11, csvRecord.get(10));
                preparedStatement.setString(12, csvRecord.get(11));
                preparedStatement.setString(13, csvRecord.get(12));
                preparedStatement.setString(14, csvRecord.get(13));
                preparedStatement.setString(15, csvRecord.get(14));
                if(csvRecord.get(15)!=null) {
                    preparedStatement.setFloat(16, Float.parseFloat(csvRecord.get(15)));
                }
                if(csvRecord.get(16)!=null) {
                    preparedStatement.setFloat(17, Float.parseFloat(csvRecord.get(16)));
                }
                if(csvRecord.get(17)!=null) {
                    preparedStatement.setFloat(18, Float.parseFloat(csvRecord.get(17)));
                }
                if(csvRecord.get(18) !=null) {
                    preparedStatement.setFloat(19, Float.parseFloat(csvRecord.get(18)));
                }
                preparedStatement.setString(20, csvRecord.get(19));
                preparedStatement.setString(21, csvRecord.get(20));
                preparedStatement.setString(22, csvRecord.get(21));
                int rowCount=  preparedStatement.executeUpdate();


            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

}