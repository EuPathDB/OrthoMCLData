package org.apidb.orthomcl.load.tools;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 
 */

/**
 * @author xingao
 *
 */
public class DumpGroup {

    private static final Logger logger = Logger.getLogger(DumpGroup.class);

    public static void main(String[] args) throws ClassNotFoundException,
            SQLException, IOException {
        
        if (args.length != 4) {
            System.err.println("Usage: java DumpGroup <out_file> <connection_string> <login> <password>");
            System.exit(-1);
        }
        String fileName = args[0];

        logger.info("Open connections...");

        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection(
                args[1], args[2], args[3]);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT og.name, eas.source_id, "
                + "      ot.three_letter_abbrev "
                + " FROM apidb.OrthologGroup og, dots.ExternalAaSequence eas, "
                + "  apidb.OrthomclTaxon ot, apidb.OrthologGroupAaSequence ogs "
                + " WHERE ot.taxon_id = eas.taxon_id "
                + "   AND eas.aa_sequence_id = ogs.aa_sequence_id "
                + "   AND ogs.ortholog_group_id = og.ortholog_group_id "
                + " ORDER BY og.ortholog_group_id ASC, "
                + "          ot.three_letter_abbrev ASC, eas.source_id ASC");
        Map<String, List<String>> groups = new LinkedHashMap<String, List<String>>();

        logger.info("Reading groups...");

        while (rs.next()) {
            String groupName = rs.getString("name");
            String sourceId = rs.getString("source_id");
            String taxon = rs.getString("three_letter_abbrev");
            List<String> group = groups.get(groupName);
            if (group == null) {
                group = new ArrayList<String>();
                groups.put(groupName, group);
            }
            group.add(taxon.trim() + "|" + sourceId.trim());
        }
        rs.close();
        stmt.close();
        connection.close();

        logger.info("Writing groups...");

        PrintWriter writer = new PrintWriter(new FileWriter(fileName));
        for (String groupName : groups.keySet()) {
            writer.print(groupName + ": ");
            List<String> group = groups.get(groupName);
            boolean first = true;
            for (String gene : group) {
                if (first) first = false;
                else writer.print("; ");
                writer.print(gene);
            }
            writer.println();
            writer.flush();
        }
        writer.close();

        logger.info("Done.");
    }
}
