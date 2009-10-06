/**
 * 
 */
package org.apidb.orthomcl.load.plugin.biolayout;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import oracle.jdbc.OracleStatement;
import oracle.sql.BLOB;
import oracle.sql.CLOB;

import org.apache.log4j.Logger;
import org.apidb.orthomcl.load.plugin.OrthoMCLException;
import org.apidb.orthomcl.load.plugin.Plugin;

/**
 * @author xingao
 * 
 */
public class GenerateBioLayoutPlugin implements Plugin {

    public static final int MAX_GROUP_SIZE = 500;

    private static final Logger logger = Logger.getLogger(GenerateBioLayoutPlugin.class);

    private GroupLoader loader;
    private Object processor;
    private Method saveMethod;

    private Connection connection;
    private PreparedStatement psUpdateImage;

    private File signalFile;

    public GenerateBioLayoutPlugin() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        initialize();
    }

    private void initialize() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        logger.debug("initializing...");
        processor = null;
        saveMethod = null;
        System.runFinalization();
        System.gc();

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
    public void setArgs(String[] args) throws OrthoMCLException {
        if (args.length != 4) {
            throw new OrthoMCLException("The args should be: "
                    + "<signal_file> <connection_string> <login> <password>");
        }

        String signalFileName = args[0];
        String connectionString = args[1];
        String login = args[2];
        String password = args[3];

        try {
            // create connection
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            connection = DriverManager.getConnection(connectionString, login,
                    password);
            signalFile = new File(signalFileName);
            if (signalFile.exists()) signalFile.delete();

            loader = new GroupLoader(connection);
        } catch (SQLException ex) {
            throw new OrthoMCLException(ex);
        } catch (ClassNotFoundException ex) {
            throw new OrthoMCLException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#invoke()
     */
    public void invoke() throws Exception {
        // prepare sqls
        prepareQueries();

        logger.debug("Getting unfinished groups...");
        Statement stGroup = connection.createStatement();
        ((OracleStatement) stGroup).setRowPrefetch(100);
        ResultSet rsGroup = stGroup.executeQuery("SELECT og.name, "
                + "      og.ortholog_group_id, og.number_of_members "
                + " FROM apidb.OrthologGroup og "
                + " WHERE biolayout_image IS NULL "
                + "   AND number_of_members <= " + MAX_GROUP_SIZE
                + "   AND number_of_members > 1 "
                + " ORDER BY number_of_members ASC");
        int groupCount = 0;
        int sequenceCount = 0;
        boolean hasMore = false;
        while (rsGroup.next()) {
            int groupId = rsGroup.getInt("ortholog_group_id");
            Group group = loader.getGroup(groupId);
            group.name = rsGroup.getString("name");
            sequenceCount += rsGroup.getInt("number_of_members");

            logger.debug("creating biolayout...");
            createLayout(group);

            groupCount++;
            if (groupCount % 10 == 0) {
                logger.debug(groupCount + " groups created...");
            }

            // only run 10000 seqs for each run
            if (sequenceCount >= 10000) {
                // hasMore = true;
                // break;
                sequenceCount = 0;
                initialize();
            }
        }
        logger.info("Total " + groupCount + " groups created.");
        rsGroup.close();
        stGroup.close();
        psUpdateImage.close();

        // create signal id finished
        if (!hasMore) signalFile.createNewFile();
    }

    private void prepareQueries() throws SQLException {
        psUpdateImage = connection.prepareStatement("UPDATE "
                + "  apidb.OrthologGroup "
                + " SET biolayout_image = ?, svg_content = ? "
                + " WHERE ortholog_group_id = ?");
    }

    private void createLayout(Group group) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException, SQLException,
            IOException {
        CLOB clob = CLOB.createTemporary(connection, false,
                CLOB.DURATION_SESSION);
        OutputStream svgStream = clob.setAsciiStream(1);
        BLOB blob = BLOB.createTemporary(connection, false,
                BLOB.DURATION_SESSION);
        OutputStream imgStream = blob.setBinaryStream(1);

        // TEST
        // logger.debug("Group: " + groupName);
        // OutputStream svgStream = new FileOutputStream(new File("/tmp/"
        // + groupName + ".svg"));
        // OutputStream imgStream = new FileOutputStream(new File("/tmp/"
        // + groupName + ".png"));

        saveMethod.invoke(processor, group, svgStream, imgStream);

        svgStream.close();
        imgStream.close();

        // save the layout and image though BioLayout
        psUpdateImage.setBlob(1, blob);
        psUpdateImage.setClob(2, clob);
        psUpdateImage.setInt(3, group.groupId);
        // psUpdateImage.addBatch();
        psUpdateImage.execute();

        logger.debug("image & svg saved.");
    }
}
