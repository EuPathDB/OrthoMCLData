import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.sql.CLOB;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xingao
 *
 */
public class FixMsaData {

    private static final int BASE = 70612;

    private static final Logger logger = Logger.getLogger(FixMsaData.class);

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java FixMsaData <msa_dir>");
            System.exit(-1);
        }
        File msaDir = new File(args[0]);

        logger.info("Making connections...");

        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection(
                "jdbc:oracle:oci:@orthomcl", "jerric", "bdomsalp");

        // get sequence map
        logger.info("Getting sequence names...");
        Map<String, String> sequences = getSequences(connection);

        logger.info("Getting svg content...");

        PreparedStatement psUpdate = connection.prepareStatement("UPDATE "
                + " apidb.OrthologGroup SET multiple_sequence_alignment = ? "
                + " WHERE ortholog_group_id = ?");

        Statement stSelect = connection.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = stSelect.executeQuery("SELECT name, "
                + " ortholog_group_id FROM apidb.OrthologGroup "
                + " WHERE multiple_sequence_alignment IS NULL");
        int count = 0;
        while (resultSet.next()) {
            int groupId = resultSet.getInt("ortholog_group_id");
            String name = resultSet.getString("name");

            List<String> contents = getContent(name, msaDir);
            if (contents.size() > 0) {
                String content = fixContent(contents, sequences);

                // update clob
                CLOB clob = CLOB.createTemporary(connection, false,
                        CLOB.DURATION_SESSION);
                clob.setString(1, content);
                psUpdate.setClob(1, clob);
                psUpdate.setInt(2, groupId);
                psUpdate.addBatch();
                
                count++;
            }

            if (count % 100 == 0) {
                psUpdate.executeBatch();
                logger.info(count + " groups updated.");
            }
        }
        if (count % 100 != 0) psUpdate.executeBatch();

        resultSet.close();
        stSelect.close();
        psUpdate.close();

        connection.close();
        System.out.println("Total " + count + " groups updated.");
    }

    private static Map<String, String> getSequences(Connection connection)
            throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT eas.aa_sequence_id, "
                + "      eas.source_id, ot.three_letter_abbrev "
                + " FROM dots.ExternalAaSequence eas, apidb.OrthomclTaxon ot, "
                + "      apidb.OrthologGroupAaSequence ogs "
                + " WHERE eas.aa_sequence_id = ogs.aa_sequence_id "
                + "   AND eas.taxon_id = ot.taxon_id");
        Map<String, String> sequences = new HashMap<String, String>();
        while (rs.next()) {
            String sequenceId = rs.getString("aa_sequence_id");
            String sourceId = rs.getString("source_id");
            String taxonName = rs.getString("three_letter_abbrev");
            String name = taxonName + "|" + sourceId;
            sequences.put(sequenceId, name.intern());
        }
        rs.close();
        stmt.close();
        return sequences;
    }

    private static List<String> getContent(String newName, File msaDir)
            throws IOException {
        int id = Integer.parseInt(newName.substring(4));
        id -= BASE;
        String oldName = "ORTHOMCL" + id + ".msa";
        List<String> content = new ArrayList<String>();

        File msaFile = new File(msaDir, oldName);
        if (!msaFile.exists()) {
            logger.warn("MSA not found for old name: " + oldName
                    + ", new name: " + newName);
            return content;
        }
        String line;
        BufferedReader reader = new BufferedReader(new FileReader(msaFile));
        while ((line = reader.readLine()) != null) {
            content.add(line);
        }
        reader.close();
        return content;
    }

    private static String fixContent(List<String> contents,
            Map<String, String> sequences) {
        // get id mapping
        int newMax = 0;
        int oldOffset = 0;
        for (String line : contents) {
            if (line.matches("\\S+\\s{5,}\\S.*")) {
                String[] parts = line.split("\\s{5,}", 2);

                String oldId = parts[0];
                String newId = sequences.get(oldId);
                if (newId == null) newId = oldId;

                if (newId.length() > newMax) newMax = newId.length();

                int offset = line.indexOf(parts[1], oldId.length() + 4);
                if (offset > oldOffset) oldOffset = offset;
            }
        }
        int newOffset = newMax + 10; // get the offset of new alignments
        if (newOffset < oldOffset) newOffset = oldOffset;

        // generate output
        StringBuffer buffer = new StringBuffer();
        for (String line : contents) {
            if (line.matches("\\S+\\s{5,}\\S.*")) {
                String[] parts = line.split("\\s{5,}", 2);

                String oldId = parts[0];
                String newId = sequences.get(oldId);
                if (newId == null) newId = oldId;

                buffer.append(newId);
                for (int i = newId.length(); i < newOffset; i++) {
                    buffer.append(' ');
                }
                buffer.append(parts[1]);
            } else if (line.matches("\\s{6,}.*")) {
                for (int i = oldOffset; i < newOffset; i++) {
                    buffer.append(' ');
                }
                buffer.append(line);
            } else {
                buffer.append(line);
            }
            buffer.append("\n");
        }
        return buffer.toString();
    }
}
