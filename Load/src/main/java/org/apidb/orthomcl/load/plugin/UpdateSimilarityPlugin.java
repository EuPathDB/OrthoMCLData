package org.apidb.orthomcl.load.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.ConnectionPoolConfig;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;

/**
 * @author xingao 
 * 
 * The example blast similarity file: 
 * >371323 (115 subjects) 
 * Sum: 371323:533:e-150:1:277:1:277:1:277:277:277:0:
 * 0...1.......2...3.....4.5...6.7...8.9...10..11..12.13 
 *      0 - "Sum" 
 *      1 - subject id 
 *      2 - score 
 *      3 - pvalue 
 *      4 - min_subject_start 
 *      5 - max_subject_end; 
 *      6 - min_query_start; 
 *      7 - max_query_end; 
 *      8 - number_of_matches 
 *      9 - total_match_length 
 *      10 - number_identical 
 *      11 - number_positive 
 *      12 - is_reversed 
 *      13 - reading_frame 
 * HSP1: 371323:277:277:277:533:e-150:1:277:1:277:0:
 * 0....1.......2...3...4...5...6.....7.8...9.10..11.12 
 *       0 - "HSPx" 
 *       1 - subject id 
 *       2 - number_identical 
 *       3 - number_positive 
 *       4 - match_length
 *       5 - score 
 *       6 - pvalue 
 *       7 - subject_start 
 *       8 - subject_end 
 *       9 - query_start 
 *       10 - query_end 
 *       11 - is_reversed 
 *       12 - reading frame 
 * Sum: 211085:96:4e-19:1014:1637:1:257:6:1437:348:712:0: 
 * HSP1: 211085:63:122:239:96:4e-19:1149:1384:3:240:0: 
 * HSP2: 211085:66:123:238:92:5e-18:1233:1467:3:239:0:
 */
public class UpdateSimilarityPlugin implements Plugin {

    private class Segment implements Comparable<Segment> {
        public int Start;
        public int Length;

        Segment(int start, int length) {
            Start = start;
            Length = length;
        }

        @Override
        public int compareTo(Segment o) {
            int startDiff = this.Start - o.Start;
            return (startDiff == 0) ? (o.Length - this.Length) : startDiff;
        }
    }

    private static final Logger LOG = Logger.getLogger(UpdateSimilarityPlugin.class);

    private ConnectionPoolConfig _dbConfig;
    private File _similarityFile;
    private String _sequenceTable;

    @Override
    public void invoke() throws OrthoMCLException {
        // prepare the statement
        BufferedReader reader = null;
        PreparedStatement psUpdate = null;
        try (DatabaseInstance db = new DatabaseInstance(_dbConfig);
             Connection connection = db.getDataSource().getConnection()) {
            psUpdate = connection.prepareStatement("UPDATE"
                    + " dots.Similarity SET non_overlap_match_length = ?"
                    + " WHERE query_id = ? AND subject_id = ?");
            reader = new BufferedReader(new FileReader(
                    _similarityFile));

            LOG.info("Loading sequence lengths...");
            Map<String, Integer> lengthMap = getLengthMap(connection);

            LOG.info("Updating non-overlap match lengths...");
            String line = null, queryId = null, subjectId = null;
            boolean useQuery = true;
            List<Segment> segments = null;
            int updateCount = 0;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                if (lineCount % 100000 == 0)
                    LOG.debug("Read " + lineCount + " lines.");

                line = line.trim();
                if (line.startsWith(">")) {
                    // update previous query-subjet
                    if (segments != null && segments.size() > 1)
                        if (update(queryId, subjectId, segments, psUpdate)) {
                            updateCount++;
                            if (updateCount % 1000 == 0) {
                                psUpdate.executeBatch();
                                LOG.info(updateCount + " pairs updated.");
                            }
                        }

                    // get next query id
                    queryId = line.substring(1, line.indexOf('(')).trim();
                    subjectId = null;
                    segments = null;
                } else if (line.startsWith("Sum")) {
                    // update previous query-subject
                    if (segments != null && segments.size() > 1)
                        if (update(queryId, subjectId, segments, psUpdate)) {
                            updateCount++;
                            if (updateCount % 1000 == 0) {
                                psUpdate.executeBatch();
                                LOG.info(updateCount + " pairs updated.");
                            }
                        }

                    // get next subject
                    String[] parts = line.split(":");
                    subjectId = parts[1].trim();
                    segments = null;
                    int matchCount = Integer.parseInt(parts[8].trim());
                    // only update the pairs with more than one match
                    if (matchCount > 1) {
                        int queryLength = lengthMap.get(queryId);
                        int subjectLength = lengthMap.get(subjectId);
                        useQuery = (queryLength >= subjectLength);
                        segments = new ArrayList<Segment>();
                    }
                } else if (line.startsWith("HSP")) {
                    if (segments != null) {
                        String[] parts = line.split(":");
                        int start, end;
                        if (useQuery) {
                            start = Integer.parseInt(parts[9]);
                            end = Integer.parseInt(parts[10]);
                        } else {
                            start = Integer.parseInt(parts[7]);
                            end = Integer.parseInt(parts[8]);
                        }
                        segments.add(new Segment(start, end - start + 1));
                    }
                }
            }
            if (segments != null && segments.size() > 1)
                if (update(queryId, subjectId, segments, psUpdate)) {
                    updateCount++;
                    if (updateCount % 1000 == 0) {
                        psUpdate.executeBatch();
                        LOG.info(updateCount + " pairs updated.");
                    }
                }

            // commit remaining updates
            if (updateCount % 1000 != 0) psUpdate.executeBatch();
            LOG.info("Total " + updateCount + " rows updated.");
            
        }
        catch (Exception ex) {
            throw new OrthoMCLException(ex);
        }
        finally {
            SqlUtils.closeQuietly(psUpdate);
            IoUtil.closeQuietly(reader);
        }
    }

    @Override
    public void setArgs(String[] args) throws OrthoMCLException {
        // verify the args
        if (args.length != 5) {
            throw new OrthoMCLException(
                    "The args should be: <sequence_table> <similarity_file> "
                            + "<connection_string> <login> <password>");
        }
        _sequenceTable = args[0];
        String similarityFileName = args[1];
        String connectionString = args[2];
        String login = args[3];
        String password = args[4];

        _dbConfig = SimpleDbConfig.create(SupportedPlatform.ORACLE, connectionString, login, password);

        _similarityFile = new File(similarityFileName);
        if (!_similarityFile.exists() || !_similarityFile.isFile()) {
          throw new OrthoMCLException(new FileNotFoundException(similarityFileName));
        }
    }

    private Map<String, Integer> getLengthMap(Connection connection) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
          stmt = connection.createStatement();
          rs = stmt.executeQuery("SELECT aa_sequence_id, length FROM " + _sequenceTable);
          Map<String, Integer> lengthMap = new HashMap<String, Integer>();
          while (rs.next()) {
            String sequenceId = rs.getString("aa_sequence_id");
            int length = rs.getInt("length");
            lengthMap.put(sequenceId, length);
          }
          return lengthMap;
        }
        finally {
          if (rs != null) rs.close();
          if (stmt != null) stmt.close();
        }
    }

    private boolean update(String queryId, String subjectId,
            List<Segment> segments, PreparedStatement psUpdate)
            throws SQLException {
        Collections.sort(segments);
        int totalLength = 0;
        int pos = 0;
        boolean overlap = false;
        for (Segment segment : segments) {
            int nextPos = segment.Start + segment.Length;
            if (nextPos <= pos) continue;

            if (segment.Start >= pos) {
                totalLength += segment.Length;
            } else {// has overlap
                totalLength += nextPos - pos;
                overlap = true;
            }
            pos = nextPos;
        }
        if (overlap) {
            //logger.debug("Updating queryId: " + queryId + ", subjectId: "
            //        + subjectId + ", match-length: " + totalLength);

            psUpdate.setInt(1, totalLength);
            psUpdate.setInt(2, Integer.parseInt(queryId));
            psUpdate.setInt(3, Integer.parseInt(subjectId));
            psUpdate.addBatch();
        }
        return overlap;
    }
}
