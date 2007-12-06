/**
 * 
 */
package org.apidb.orthomcl.load.plugin;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * @author xingao
 *
 */
public class GenerateBioLayoutPlugin implements Plugin {

    private class Pair {
        int queryId;
        int subjectId;

        public Pair(int queryId, int subjectId) {
            this.queryId = queryId;
            this.subjectId = subjectId;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Pair) {
                Pair p = (Pair) obj;
                return this.queryId == p.queryId
                        && this.subjectId == p.subjectId;
            } else return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return queryId ^ subjectId;
        }

    }

    private static final Logger logger = Logger.getLogger(GenerateBioLayoutPlugin.class);

    private Object hiddenFrame;
    private Method saveMethod;

    private Connection connection;
    private String sequenceTable;
    private File inDir;
    private File outDir;
    private File imgDir;

    public GenerateBioLayoutPlugin() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        Class<?> frameClass = Class.forName("HiddenLayoutFrame");
        hiddenFrame = frameClass.newInstance();

        // get the handle to the method
        Class<?>[] params = { File.class, File.class, File.class };
        saveMethod = hiddenFrame.getClass().getDeclaredMethod("saveFile",
                params);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#setArgs(java.lang.String[])
     */
    @Override
    public void setArgs(String[] args) throws OrthoMCLException {
        if (args.length != 5) {
            throw new OrthoMCLException("The args should be: <sequence_table> "
                    + "<bl_output_dir> <connection_string> <login> <password>");
        }

        sequenceTable = args[0];
        String blDirName = args[1];
        String connectionString = args[2];
        String login = args[3];
        String password = args[4];

        try {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            connection = DriverManager.getConnection(connectionString, login,
                    password);
            File blDir = new File(blDirName);
            if (!blDir.exists() || !blDir.isDirectory())
                throw new FileNotFoundException(blDirName);

            // prepare directory structure
            inDir = new File(blDir, "/input/");
            if (!inDir.exists()) inDir.mkdirs();
            outDir = new File(blDir, "/master/mainresult/bl/");
            if (!outDir.exists()) outDir.mkdirs();
            imgDir = new File(blDir, "/master/mainresult/img/");
            if (!imgDir.exists()) imgDir.mkdirs();
        } catch (SQLException ex) {
            throw new OrthoMCLException(ex);
        } catch (FileNotFoundException ex) {
            throw new OrthoMCLException(ex);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#invoke()
     */
    @Override
    public void invoke() throws OrthoMCLException {
        try {
            PreparedStatement psSimilarity = connection.prepareStatement("SELECT "
                    + "  s.query_id, s.subject_id, s.pvalue_mant, s.pvalue_exp "
                    + " FROM dots.Similarity s "
                    + " WHERE query_id != subject_id "
                    + "   AND query_id IN (SELECT aa_sequence_id "
                    + "                    FROM apidb.OrthologGroupAaSequence "
                    + "                    WHERE ortholog_group_id = ?) "
                    + " AND subject_id IN (SELECT aa_sequence_id "
                    + "                    FROM apidb.OrthologGroupAaSequence "
                    + "                    WHERE ortholog_group_id = ?) ");
            PreparedStatement psSequence = connection.prepareStatement("SELECT"
                    + "      ogs.aa_sequence_id, eas.taxon_id "
                    + " FROM apidb.OrthologGroupAaSequence ogs, "
                    + sequenceTable + " eas "
                    + " WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                    + "   AND ogs.ortholog_group_id = ?");

            Statement stGroup = connection.createStatement();
            ResultSet rsGroup = stGroup.executeQuery("SELECT og.name, "
                    + "      og.ortholog_group_id "
                    + " FROM apidb.OrthologGroup og ");
            int groupCount = 0;
            while (rsGroup.next()) {
                int groupId = rsGroup.getInt("ortholog_group_id");
                String groupName = rsGroup.getString("name");
                createLayout(groupId, groupName, psSequence, psSimilarity);

                groupCount++;
                if (groupCount % 100 == 0) {
                    logger.debug(groupCount + " groups created...");
                }
            }
            logger.info("Total " + groupCount + " groups created at "
                    + outDir.getAbsolutePath());
            rsGroup.close();
            stGroup.close();
            psSimilarity.close();
        } catch (SQLException ex) {
            throw new OrthoMCLException(ex);
        } catch (IllegalArgumentException ex) {
            throw new OrthoMCLException(ex);
        } catch (IllegalAccessException ex) {
            throw new OrthoMCLException(ex);
        } catch (InvocationTargetException ex) {
            throw new OrthoMCLException(ex);
        } catch (IOException ex) {
            throw new OrthoMCLException(ex);
        }
    }

    private void createLayout(int groupId, String groupName,
            PreparedStatement psSequence, PreparedStatement psSimilarity)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, SQLException, IOException {
        // decide output file
        File inFile = new File(inDir, groupName + ".in");
        File outFile = new File(outDir, groupName + ".bl");
        File imgFile = new File(imgDir, groupName + ".png");

        PrintWriter inWriter = new PrintWriter(new FileWriter(inFile));

        // extract pairs and create input file
        extractPairs(groupId, psSimilarity, inWriter);

        // extract node classes and append to the input file
        extractNodes(groupId, psSequence, inWriter);

        inWriter.close();

        // save the layout and image though BioLayout
        saveMethod.invoke(hiddenFrame, inFile, outFile, imgFile);
    }

    private void extractPairs(int groupId, PreparedStatement psSimilarity,
            PrintWriter inWriter) throws SQLException {
        // get all pairs
        psSimilarity.setInt(1, groupId);
        psSimilarity.setInt(2, groupId);
        ResultSet rsSimilarity = psSimilarity.executeQuery();
        Map<Pair, Double> pairs = new HashMap<Pair, Double>();
        while (rsSimilarity.next()) {
            int queryId = rsSimilarity.getInt("query_id");
            int subjectId = rsSimilarity.getInt("subject_id");
            double pValueMant = rsSimilarity.getDouble("pvalue_mant");
            int pValueExp = rsSimilarity.getInt("pvalue_exp");
            double score = (pValueMant == 0) ? 181
                    : (-Math.log10(pValueMant) - pValueExp);
            pairs.put(new Pair(queryId, subjectId), score);
        }
        rsSimilarity.close();

        // output both-way pairs
        Set<Pair> skipPairs = new HashSet<Pair>();
        for (Pair pair : pairs.keySet()) {
            // skip the pair that has been used
            if (skipPairs.contains(pair)) continue;
            
            // check if backward pair exists; if not, skip to the next
            Pair backPair = new Pair(pair.subjectId, pair.queryId);
            if (pairs.containsKey(backPair)) {
                skipPairs.add(backPair);
                double score = (pairs.get(pair) + pairs.get(backPair)) / 2;
                inWriter.println(String.format("%1$d\t%2$d\t%3$6.2f",
                        pair.queryId, pair.subjectId, score));
            }
        }
        inWriter.flush();
    }

    private void extractNodes(int groupId, PreparedStatement psSequence,
            PrintWriter inWriter) throws SQLException {
        psSequence.setInt(1, groupId);
        ResultSet rsSequence = psSequence.executeQuery();
        while (rsSequence.next()) {
            int sequenceId = rsSequence.getInt("aa_sequence_id");
            int taxonId = rsSequence.getInt("taxon_id");
            inWriter.println("//NODECLASS\t" + sequenceId + "\t" + taxonId);
        }
        rsSequence.close();
        inWriter.flush();
    }
}
