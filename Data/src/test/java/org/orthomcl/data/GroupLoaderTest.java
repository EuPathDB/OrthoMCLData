package org.orthomcl.data;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Assert;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.TestUtil;
import org.gusdb.fgputil.runtime.GusHome;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class GroupLoaderTest {

    private static final Logger logger = Logger.getLogger(GroupLoaderTest.class.getName());

    private static final String DB_CREATION_SCRIPT = "org/orthomcl/data/buildTestDb.sql";
    private static final String DB_DELETION_SCRIPT = "org/orthomcl/data/destroyTestDb.sql";

    private static final boolean RUN_AGAINST_LIVE_DB = false;
    
    private DataSource testDb;
    private GroupLoader loader;

    @Before
    public void setUpTest() throws Exception {
        testDb = (RUN_AGAINST_LIVE_DB ? getLiveDataSource() : getTestDataSource());
        loader = new GroupLoader(testDb.getConnection());
    }
    
    @Test
    public void testLoadGroup() {
        //testLoadGroup("OG5_210000", 2, 1);
        //testLoadGroup("OG5_200000", 3, 3);
        //testLoadGroup("OG5_180000", 4, 6);
        //testLoadGroup("OG5_126588", 469, 104651);
        //testLoadGroup("OG5_126587", 480, 106052);
        //testLoadGroup("OG5_126586", 481, 114961);
    }

    @SuppressWarnings("unused")
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
        Assert.assertEquals(0, count);
        //Assert.assertEquals(150, count);
    }
    
    @After
    public void cleanDb() throws Exception {
      TestUtil.runSqlScript(testDb, DB_DELETION_SCRIPT);
    }
    
    private static class AppDbProperties {
      public String url, login, password;
    }
    
    private static DataSource getLiveDataSource() throws IOException, SQLException, ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");
        String gusHome = GusHome.getGusHome();
        logger.debug("Found GUS home: " + gusHome);
        File configFile = new File(gusHome + "/config/OrthoMCL/model-config.xml");
        logger.debug("Config File: " + configFile.getAbsolutePath());
        AppDbProperties dbProps = parseAppDbProperties(configFile);
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(dbProps.url);
        ds.setUser(dbProps.login);
        ds.setPassword(dbProps.password);
        return ds;
        //return DriverManager.getConnection(dbProps.url, dbProps.login, dbProps.password);
    }
    
    private static AppDbProperties parseAppDbProperties(File modelConfigFile) throws IOException {
      byte[] buffer = new byte[(int) modelConfigFile.length()];
      InputStream input = new FileInputStream(modelConfigFile);
      input.read(buffer, 0, buffer.length);
      String content = new String(buffer);
      int pos = content.indexOf("<appDb");
      content = content.substring(pos + 6, content.indexOf("/>", pos + 1));
      String[] parts = content.trim().split("\\s*['\"]\\s*");
      AppDbProperties props = new AppDbProperties();
      for (int i = 0; i < parts.length - 1; i += 2) {
          if (parts[i].equals("connectionUrl=")) props.url = parts[i + 1];
          else if (parts[i].equals("login=")) props.login = parts[i + 1];
          else if (parts[i].equals("password=")) props.password = parts[i + 1];
      }
      return props;
    }
    
    private static DataSource getTestDataSource() throws SQLException, IOException {
      DataSource ds = TestUtil.getTestDataSource("myinmemdb");
      TestUtil.runSqlScript(ds, DB_CREATION_SCRIPT);
      return ds;
    }
    
}
