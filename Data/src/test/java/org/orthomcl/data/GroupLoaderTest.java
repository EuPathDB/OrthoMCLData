package org.orthomcl.data;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class GroupLoaderTest {

    private static final Logger logger = Logger.getLogger(GroupLoaderTest.class);

    private final GroupLoader loader;

    public GroupLoaderTest() throws SQLException, ClassNotFoundException,
            IOException {
        Class.forName("oracle.jdbc.OracleDriver");
        Connection connection = createConnection();
        loader = new GroupLoader(connection);
    }

    private Connection createConnection() throws IOException, SQLException {
    	String gusHome = System.getenv("GUS_HOME");
    	logger.debug("Found GUS home: " + gusHome);
        File configFile = new File(gusHome + "/config/OrthoMCL/model-config.xml");
        logger.debug("Config File: " + configFile.getAbsolutePath());
        byte[] buffer = new byte[(int) configFile.length()];
        InputStream input = new FileInputStream(configFile);
        input.read(buffer, 0, buffer.length);
        String content = new String(buffer);
        int pos = content.indexOf("<appDb");
        content = content.substring(pos + 6, content.indexOf("/>", pos + 1));
        String[] parts = content.trim().split("\\s*['\"]\\s*");
        String url = null, login = null, password = null;
        for (int i = 0; i < parts.length - 1; i += 2) {
            if (parts[i].equals("connectionUrl=")) url = parts[i + 1];
            else if (parts[i].equals("login=")) login = parts[i + 1];
            else if (parts[i].equals("password=")) password = parts[i + 1];
        }
        return DriverManager.getConnection(url, login, password);
    }

    @Test
    public void testLoadGroup() throws SQLException, OrthoMCLDataException, IOException {
        //testLoadGroup("OG5_210000", 2, 1);
        //testLoadGroup("OG5_200000", 3, 3);
        //testLoadGroup("OG5_180000", 4, 6);
        //testLoadGroup("OG5_126588", 469, 104651);
        //testLoadGroup("OG5_126587", 480, 106052);
        //testLoadGroup("OG5_126586", 481, 114961);
    }

    private void testLoadGroup(String name, int geneCount, int scoreCount)
            throws SQLException, OrthoMCLDataException, IOException {
        long start = System.currentTimeMillis();
        byte[] group = loader.getGroupData(name);
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(group));
        
        // read compressed layout data length
        Assert.assertEquals(geneCount, input.readInt());
        int layoutLength = input.readInt();
        Assert.assertTrue(layoutLength >= 0);
        
        // skip layout data & read gene count
        input.skip(layoutLength);
        
        double spent = (System.currentTimeMillis() - start) / 1000D;
        logger.debug("Group " + name + " loaded in " + spent + " seconds.");
    }
    
    @Test
    public void testLoadOrganisms() throws SQLException, IOException {
        byte[] organisms = loader.getOrganismsData();
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(organisms));
        int count = input.readInt();
        Assert.assertEquals(150, count);
    }
}
