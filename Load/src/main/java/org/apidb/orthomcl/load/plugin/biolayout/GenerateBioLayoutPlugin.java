package org.apidb.orthomcl.load.plugin.biolayout;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apidb.orthomcl.load.plugin.OrthoMCLException;
import org.apidb.orthomcl.load.plugin.Plugin;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;

/**
 * @author xingao
 */
@Deprecated
public class GenerateBioLayoutPlugin implements Plugin {

    private static final Logger logger = Logger.getLogger(GenerateBioLayoutPlugin.class);

    public static final int MAX_GROUP_SIZE = 500;
    
    private Connection connection;
    
    private GroupLoader loader;
    private Object processor;
    private Method saveMethod;

    public GenerateBioLayoutPlugin() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        initialize();
    }

    private void initialize() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        logger.debug("initializing...");

        Class<?> processorClass = Class.forName("BiolayoutProcessor");
        processor = processorClass.newInstance();

        // get the handle to the method
        Class<?>[] params = { Group.class, OutputStream.class,
                OutputStream.class };
        saveMethod = processor.getClass().getDeclaredMethod("saveData", params);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#setArgs(java.lang.String[])
     */
    @Override
    public void setArgs(String[] args) throws OrthoMCLException {
        if (args.length != 3) {
            throw new OrthoMCLException("The args should be: "
                    + "<connection_string> <login> <password>");
        }

        String connectionString = args[0];
        String login = args[1];
        String password = args[2];

        try {
            // create connection
            DatabaseInstance db = new DatabaseInstance("DB", SimpleDbConfig.create(
                SupportedPlatform.ORACLE, connectionString, login, password));
            connection = db.getDataSource().getConnection();
            loader = new GroupLoader(connection);
        } catch (SQLException ex) {
            throw new OrthoMCLException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#invoke()
     */
    @Override
    public void invoke() throws Exception {
      Map<Integer, String> groups = getGroups();
      PreparedStatement psUpdateImage = null;
      try {
        logger.debug(groups.size() + " groups to be processed.");
        int groupCount = 0;
        int sequenceCount = 0;
        psUpdateImage = connection.prepareStatement("UPDATE "
            + "  apidb.OrthologGroup "
            + " SET biolayout_image = ?, svg_content = ? "
            + " WHERE ortholog_group_id = ?");
          
        for (int groupId : groups.keySet()) {
          Group group = loader.getGroup(groupId);
          group.name = groups.get(groupId);
          sequenceCount += group.nodes.size();

          // logger.debug("creating biolayout...");
          createLayout(group, psUpdateImage);

          groupCount++;
          if (groupCount % 10 == 0) {
            logger.debug(groupCount + " groups created...");
          }

          // only run 10000 seqs for each run
          if (sequenceCount >= 10000) {
            sequenceCount = 0;
            // initialize();
          }
        }
        logger.info("Total " + groupCount + " groups created.");
      }
      finally {
        SqlUtils.closeQuietly(psUpdateImage);
      }
    }

    private Map<Integer, String> getGroups() throws SQLException {
      Statement stGroup = null;
      ResultSet rsGroup = null;
      try {
        // prepare sqls
        logger.debug("Getting unfinished groups...");
        stGroup = connection.createStatement();
        rsGroup = stGroup.executeQuery("SELECT "
            + "      ortholog_group_id, name "
            + " FROM apidb.OrthologGroup "
            + " WHERE biolayout_image IS NULL "
            + "   AND number_of_members <= " + MAX_GROUP_SIZE
            + "   AND number_of_members > 1 "
            + "ORDER BY number_of_members ASC");
        Map<Integer, String> groups = new LinkedHashMap<Integer, String>();
        while (rsGroup.next()) {
            int groupId = rsGroup.getInt("ortholog_group_id");
            String name = rsGroup.getString("name");
            groups.put(groupId, name);
        }
        return groups;
      }
      finally {
        SqlUtils.closeQuietly(rsGroup, stGroup);
      }
    }

    /**
     * 
     * 
     * @param group
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws SQLException
     * @throws IOException
     */
    private void createLayout(Group group, PreparedStatement psUpdateImage) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException, SQLException,
            IOException {
        // Create temporary file names for files to hold SVG and PNG data
        String tmpSvgFile = "/tmp/GenerateBioLayoutPlugin-" + UUID.randomUUID() + ".svg";
        String tmpPngFile = "/tmp/GenerateBioLayoutPlugin-" + UUID.randomUUID() + ".png";

        // Write files to /tmp
        try (FileOutputStream svgStream = new FileOutputStream(tmpSvgFile);
             FileOutputStream pngStream = new FileOutputStream(tmpPngFile)) {
          saveMethod.invoke(processor, group, svgStream, pngStream);
        }
        
        // Read files from /tmp into DB
        try (FileReader svgReader = new FileReader(tmpSvgFile);
             FileInputStream pngStream = new FileInputStream(tmpPngFile);) {
          // save the layout and image though BioLayout
          psUpdateImage.setBlob(1, pngStream);
          psUpdateImage.setClob(2, svgReader);
          psUpdateImage.setInt(3, group.groupId);
          // psUpdateImage.addBatch();
          psUpdateImage.execute();
          // logger.debug("image & svg saved.");
        }
    }
}
