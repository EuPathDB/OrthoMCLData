/**
 * 
 */
package org.apidb.orthomcl.load.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import java.util.HashMap;
import java.util.Map;

import oracle.sql.BLOB;
import oracle.sql.CLOB;

import org.apache.log4j.Logger;

/**
 * @author xingao
 * 
 */
public class GenerateBioLayoutPlugin implements Plugin {

    public static class Taxon {

        public int Id;
        public String Abbreviation;
        public String Name;

        public Taxon(int id, String abbreviation, String name) {
            Id = id;
            Abbreviation = abbreviation.replaceAll("\\s+", " ").trim().intern();
            Name = name.replaceAll("\\s+", " ").trim().intern();
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Taxon) {
                Taxon taxon = (Taxon) obj;
                return this.Id == taxon.Id;
            } else return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return Id;
        }
    }

    public enum EdgeType {
        Ortholog, Coortholog, Inparalog, General
    }

    public static class Sequence {
        public int SequenceId;
        public String SourceId;
        public int TaxonId;
        public String Description;

        public Sequence(int sequenceId, String sourceId, int taxonId,
                String description) {
            SequenceId = sequenceId;
            SourceId = sourceId.replaceAll("\\s+", " ").trim().intern();
            TaxonId = taxonId;
            Description = description;
            if (Description != null) {
                Description = Description.replaceAll("\\s+", " ").trim().intern();
                Description = Description.replace('\'', '"');
            }
        }
    }

    public static class OrthomclEdge {

        public int EdgeId;
        public int QueryId;
        public int SubjectId;
        public EdgeType Type = EdgeType.General;
        public double PValueMant;
        public int PValueExp;
        /**
         * The weight is a function of pvalue, scaling into [0, 255]
         */
        public double Weight;
        public boolean TwoWay = true;

        public OrthomclEdge(int queryId, int subjectId) {
            QueryId = queryId;
            SubjectId = subjectId;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof OrthomclEdge) {
                OrthomclEdge edge = (OrthomclEdge) obj;
                return (QueryId == edge.QueryId && SubjectId == edge.SubjectId)
                        || (QueryId == edge.SubjectId && SubjectId == edge.QueryId);
            }
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return QueryId ^ SubjectId;
        }
    }

    public static final int MAX_GROUP_SZIE = 500;

    private static final Logger logger = Logger.getLogger(GenerateBioLayoutPlugin.class);

    private Object processor;
    private Method saveMethod;

    private Connection connection;
    private PreparedStatement psSequence;
    private PreparedStatement psBlast;
    private PreparedStatement psEdge;
    private PreparedStatement psUpdateImage;

    private File svgFile;
    private File signalFile;

    public GenerateBioLayoutPlugin() throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, SecurityException,
            NoSuchMethodException {
        Class<?> processorClass = Class.forName("BiolayoutProcessor");
        processor = processorClass.newInstance();

        // get the handle to the method
        Class<?>[] params = { Map.class, Map.class, Map.class, String[].class,
                OutputStream.class, OutputStream.class };
        saveMethod = processor.getClass().getDeclaredMethod("saveData", params);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apidb.orthomcl.load.plugin.Plugin#setArgs(java.lang.String[])
     */
    public void setArgs(String[] args) throws OrthoMCLException {
        if (args.length != 5) {
            throw new OrthoMCLException("The args should be: <svg_template> "
                    + "<signal_file> <connection_string> <login> <password>");
        }

        String svgFileName = args[0];
        String signalFileName = args[1];
        String connectionString = args[2];
        String login = args[3];
        String password = args[4];

        try {
            // create connection
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            connection = DriverManager.getConnection(connectionString, login,
                    password);

            // check if SVG template file exists
            svgFile = new File(svgFileName);
            if (!svgFile.exists() || !svgFile.isFile())
                throw new FileNotFoundException(svgFileName);

            signalFile = new File(signalFileName);
            if (signalFile.exists()) signalFile.delete();
        } catch (SQLException ex) {
            throw new OrthoMCLException(ex);
        } catch (IOException ex) {
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

        // read SVG template
        logger.debug("Loading SVG template...");
        String[] svgTemplate = loadSVGTemplate(svgFile);

        // load taxons
        logger.debug("Loading taxon info...");
        Map<Integer, Taxon> taxons = loadTaxons();

        logger.debug("Getting unfinished groups...");
        Statement stGroup = connection.createStatement();
        ResultSet rsGroup = stGroup.executeQuery("SELECT og.name, "
                + "      og.ortholog_group_id, og.number_of_members "
                + " FROM apidb.OrthologGroup og "
                + " WHERE biolayout_image IS NULL "
                + "   AND number_of_members <= " + MAX_GROUP_SZIE
                + "   AND number_of_members > 1 "
                + " ORDER BY number_of_members ASC");
        int groupCount = 0;
        int sequenceCount = 0;
        boolean hasMore = false;
        while (rsGroup.next()) {
            int groupId = rsGroup.getInt("ortholog_group_id");
            String groupName = rsGroup.getString("name");
            sequenceCount += rsGroup.getInt("number_of_members");

            createLayout(groupId, groupName, taxons, svgTemplate);

            groupCount++;
            if (groupCount % 10 == 0) {
                logger.debug(groupCount + " groups created...");
            }

            // only run 10000 seqs for each run
            if (sequenceCount >= 10000) {
                hasMore = true;
                break;
            }
        }
        logger.info("Total " + groupCount + " groups created.");
        rsGroup.close();
        stGroup.close();

        psBlast.close();
        psSequence.close();
        psEdge.close();
        psUpdateImage.close();

        // create signal id finished
        if (!hasMore) signalFile.createNewFile();
    }

    private void prepareQueries() throws SQLException {
        psBlast = connection.prepareStatement("SELECT "
                + "  s.evalue_mant, s.evalue_exp "
                + " FROM apidb.SimilarSequences s "
                + " WHERE (query_id = ? AND subject_id = ?) "
                + "  OR (query_id = ? AND subject_id = ?) ");
        psSequence = connection.prepareStatement("SELECT"
                + "      ogs.aa_sequence_id, eas.source_id, eas.taxon_id, "
                + "      eas.description "
                + " FROM apidb.OrthologGroupAaSequence ogs, "
                + "      dots.ExternalAaSequence eas "
                + " WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                + "   AND ogs.ortholog_group_id = ?");
        psUpdateImage = connection.prepareStatement("UPDATE "
                + "  apidb.OrthologGroup "
                + " SET biolayout_image = ?, svg_content = ? "
                + " WHERE ortholog_group_id = ?");

        String sqlPiece1 = " AS type, e.sequence_id_a AS query_id, "
                + "  e.sequence_id_b AS subject_id FROM ";
        String sqlPiece2 = " e, apidb.orthologgroupaasequence ogs_a, "
                + " apidb.orthologgroupaasequence ogs_b "
                + " WHERE ogs_a.aa_sequence_id = e.sequence_id_a "
                + "   AND ogs_b.aa_sequence_id = e.sequence_id_b "
                + "   AND ogs_a.ortholog_group_id = ? "
                + "   AND ogs_b.ortholog_group_id = ? ";
        psEdge = connection.prepareStatement("SELECT 'O'" + sqlPiece1
                + "apidb.Ortholog" + sqlPiece2 + " UNION SELECT 'C'"
                + sqlPiece1 + "apidb.Coortholog" + sqlPiece2
                + " UNION SELECT 'P'" + sqlPiece1 + "apidb.Inparalog"
                + sqlPiece2);
    }

    private String[] loadSVGTemplate(File svgFile) throws IOException {
        String[] template = new String[3];
        BufferedReader reader = new BufferedReader(new FileReader(svgFile));
        StringBuffer buffer = new StringBuffer();
        String line;

        // read header
        while ((line = reader.readLine()) != null) {
            if (line.trim().equals("$$DataSection$$")) break;
            buffer.append(line);
            buffer.append("\n");
        }
        template[0] = buffer.toString();

        // read middle part
        buffer = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            if (line.trim().equals("$$DisplaySection$$")) break;
            buffer.append(line);
            buffer.append("\n");
        }
        template[1] = buffer.toString();

        // read footer
        buffer = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            buffer.append(line);
            buffer.append("\n");
        }
        template[2] = buffer.toString();

        reader.close();
        return template;
    }

    private Map<OrthomclEdge, OrthomclEdge> getEdges(int groupId,
            Map<Integer, Sequence> sequences) throws IOException, SQLException {
        int edgeIndex = 0;
        Map<OrthomclEdge, OrthomclEdge> edges = new HashMap<OrthomclEdge, OrthomclEdge>();

        // load ortholog, coortholog and inparalog edges
        psEdge.setInt(1, groupId);
        psEdge.setInt(2, groupId);
        psEdge.setInt(3, groupId);
        psEdge.setInt(4, groupId);
        psEdge.setInt(5, groupId);
        psEdge.setInt(6, groupId);
        ResultSet rsEdge = psEdge.executeQuery();
        while (rsEdge.next()) {
            int queryId = rsEdge.getInt("query_id");
            int subjectId = rsEdge.getInt("subject_id");
            OrthomclEdge edge = new OrthomclEdge(queryId, subjectId);
            if (!edges.containsKey(edge)) {
                String type = rsEdge.getString("type");
                if (type.equals("O")) edge.Type = EdgeType.Ortholog;
                else if (type.equals("C")) edge.Type = EdgeType.Coortholog;
                else if (type.equals("P")) edge.Type = EdgeType.Inparalog;

                edge.EdgeId = ++edgeIndex;
                fillBlastScore(edge);
                edges.put(edge, edge);
            }
        }
        rsEdge.close();

        // load general edges
        Integer[] sequenceIds = new Integer[sequences.size()];
        sequences.keySet().toArray(sequenceIds);
        for (int i = 0; i < sequenceIds.length - 1; i++) {
            for (int j = i + 1; j < sequenceIds.length; j++) {
                int queryId = sequenceIds[i];
                int subjectId = sequenceIds[j];
                OrthomclEdge edge = new OrthomclEdge(queryId, subjectId);

                if (!edges.containsKey(edge)) {
                    edge.Type = EdgeType.General;
                    edge.EdgeId = ++edgeIndex;
                    fillBlastScore(edge);
                    edges.put(edge, edge);
                }
            }
        }
        return edges;
    }

    private Map<Integer, Taxon> loadTaxons() throws SQLException {
        Statement stTaxon = connection.createStatement();
        ResultSet rsTaxon = stTaxon.executeQuery("SELECT ot.taxon_id, "
                + "      ot.three_letter_abbrev, ot.name "
                + " FROM apidb.OrthomclTaxon ot WHERE ot.is_species != 0");
        Map<Integer, Taxon> taxons = new HashMap<Integer, Taxon>();
        while (rsTaxon.next()) {
            int taxonId = rsTaxon.getInt("taxon_id");
            String abbrev = rsTaxon.getString("three_letter_abbrev");
            String taxonName = rsTaxon.getString("name");
            taxons.put(taxonId, new Taxon(taxonId, abbrev, taxonName));
        }
        rsTaxon.close();
        stTaxon.close();
        return taxons;
    }

    private void createLayout(int groupId, String groupName,
            Map<Integer, Taxon> taxons, String[] svgTemplate)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, SQLException, IOException {
        // get a sequence map
        Map<Integer, Sequence> sequenceMap = getSequences(groupId);
        Map<OrthomclEdge, OrthomclEdge> edges = getEdges(groupId, sequenceMap);

        // normalize the weights to be in [0..1]
        normalizeWeights(edges);

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

        saveMethod.invoke(processor, taxons, sequenceMap, edges, svgTemplate,
                svgStream, imgStream);

        svgStream.close();
        imgStream.close();
        // System.exit(0);

        // save the layout and image though BioLayout

        psUpdateImage.setBlob(1, blob);
        psUpdateImage.setClob(2, clob);
        psUpdateImage.setInt(3, groupId);
        // psUpdateImage.addBatch();
        psUpdateImage.execute();
    }

    private Map<Integer, Sequence> getSequences(int groupId)
            throws SQLException {
        psSequence.setInt(1, groupId);
        ResultSet rsSequence = psSequence.executeQuery();
        Map<Integer, Sequence> sequenceMap = new HashMap<Integer, Sequence>();
        while (rsSequence.next()) {
            int sequenceId = rsSequence.getInt("aa_sequence_id");
            String sourceId = rsSequence.getString("source_id");
            int taxonId = rsSequence.getInt("taxon_id");
            String description = rsSequence.getString("description");
            sequenceMap.put(sequenceId, new Sequence(sequenceId, sourceId,
                    taxonId, description));
        }
        rsSequence.close();
        return sequenceMap;
    }

    private void fillBlastScore(OrthomclEdge edge) throws SQLException {
        // get all pairs
        psBlast.setInt(1, edge.QueryId);
        psBlast.setInt(2, edge.SubjectId);
        psBlast.setInt(3, edge.SubjectId);
        psBlast.setInt(4, edge.QueryId);
        ResultSet rsBlast = psBlast.executeQuery();
        int count = 0;
        double mantMax = 0;
        int expMax = Integer.MIN_VALUE;
        while (rsBlast.next()) {
            int pValueExp = rsBlast.getInt("evalue_exp");
            double pValueMant = rsBlast.getDouble("evalue_mant");
            if (expMax < pValueExp
                    || (expMax == pValueExp && mantMax < pValueMant)) {
                mantMax = pValueMant;
                expMax = pValueExp;
            }
            count++;
        }
        rsBlast.close();

        // only output 2-way matches
        edge.TwoWay = (count == 2);
        edge.PValueMant = mantMax;
        edge.PValueExp = expMax;
    }

    private void normalizeWeights(Map<OrthomclEdge, OrthomclEdge> edges) {
        // compute weights
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        for (OrthomclEdge edge : edges.values()) {
            if (edge.PValueMant == 0) {
                edge.Weight = 181;
            } else {
                edge.Weight = -Math.log10(edge.PValueMant) - edge.PValueExp;
            }
            if (edge.Weight > max) max = edge.Weight;
            if (edge.Weight < min) min = edge.Weight;
        }
        // normalize weights
        double range = (max == min) ? 1 : (max - min);
        for (OrthomclEdge edge : edges.values()) {
            edge.Weight = (edge.Weight - min) / range;
        }
    }
}
