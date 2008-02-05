import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 */

/**
 * @author xingao
 *
 */
public class ChangeGroupName {

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        final int BASE = 70612;
        
        System.out.println("Making connections...");
        
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection(
                "jdbc:oracle:oci:@orthomcl", "jerric", "bdomsalp");
        
        System.out.println("Getting names...");
        
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT name FROM apidb.OrthologGroup");
        List<String> names = new ArrayList<String>();
        while (resultSet.next()) {
            names.add(resultSet.getString("name"));
        }
        resultSet.close();
        statement.close();

        System.out.println("Updating names...");
        
        int count = 0;
        PreparedStatement psUpdate = connection.prepareStatement("UPDATE apidb.OrthologGroup SET name = ? WHERE name = ?");
        for (String name : names) {
            int id = Integer.parseInt(name.substring(8));
            String newName = "OG2_" + (id + BASE);
            psUpdate.setString(1, newName);
            psUpdate.setString(2, name);
            psUpdate.execute();
            
            count++;
            if (count % 1000 == 0) System.out.println(count + " groups updated.");
        }
        psUpdate.close();
        connection.close();
        System.out.println("Total " + count + " groups updated.");
    }

}
