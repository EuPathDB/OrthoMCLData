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
            Abbreviation = abbreviation.intern();
            Name = name.intern();
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
        BestHit, BetterHit, General
    }

    public static class Sequence {
        public int SequenceId;
        public String SourceId;
        public int TaxonId;
        public String Description;

        public Sequence(int sequenceId, String sourceId, int taxonId,
                String description) {
            SequenceId = sequenceId;
            SourceId = sourceId.intern();
            TaxonId = taxonId;
            Description = description;
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

    private static final Logger logger = Logger.getLogger(GenerateBioLayoutPlugin.class);

    private Object processor;
    private Method saveMethod;

    private Connection connection;
    private File rbhFile;
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
        if (args.length != 6) {
            throw new OrthoMCLException(
                    "The args should be: <rbh_file> "
                            + "<svg_template> <signal_file> <connection_string> <login> <password>");
        }

        String rbhFileName = args[0];
        String svgFileName = args[1];
        String signalFileName = args[2];
        String connectionString = args[3];
        String login = args[4];
        String password = args[5];

        try {
            // create connection
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            connection = DriverManager.getConnection(connectionString, login,
                    password);

            // check if RBH file exists
            rbhFile = new File(rbhFileName);
            if (!rbhFile.exists() || !rbhFile.isFile())
                throw new FileNotFoundException(rbhFileName);

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
    public void invoke() throws OrthoMCLException {
        try {
            PreparedStatement psSimilarity = connection.prepareStatement("SELECT "
                    + "  s.pvalue_mant, s.pvalue_exp "
                    + " FROM dots.Similarity s "
                    + " WHERE (query_id = ? AND subject_id = ?) "
                    + "  OR (query_id = ? AND subject_id = ?) ");
            PreparedStatement psSequence = connection.prepareStatement("SELECT"
                    + "      ogs.aa_sequence_id, eas.source_id, eas.taxon_id, "
                    + "      eas.description "
                    + " FROM apidb.OrthologGroupAaSequence ogs, "
                    + "      dots.ExternalAaSequence eas "
                    + " WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                    + "   AND ogs.ortholog_group_id = ?");
            PreparedStatement psUpdateImage = connection.prepareStatement("UPDATE "
                    + "  apidb.OrthologGroup "
                    + " SET biolayout_image = ?, svg_content = ? "
                    + " WHERE ortholog_group_id = ?");

            // read SVG template
            logger.debug("Loading SVG template...");
            String[] svgTemplate = loadSVGTemplate(svgFile);

            // read RBH file
            logger.debug("Loading RBH file...");
            Map<OrthomclEdge, OrthomclEdge> rbhEdges = loadRBHFile(rbhFile);

            // load taxons
            logger.debug("Loading taxon info...");
            Map<Integer, Taxon> taxons = loadTaxons();

            logger.debug("Getting unfinished groups...");
            Statement stGroup = connection.createStatement();
            ResultSet rsGroup = stGroup.executeQuery("SELECT og.name, "
                    + "      og.ortholog_group_id "
                    + " FROM apidb.OrthologGroup og "
                    + " WHERE biolayout_image IS NULL "
                    + "   AND number_of_members <= 500 "
                    + " ORDER BY number_of_members ASC");
            int groupCount = 0;
            boolean hasMore = false;
            while (rsGroup.next()) {
                // only run 1000 for each run
                if (groupCount >= 100) {
                    hasMore = true;
                    break;
                }
                int groupId = rsGroup.getInt("ortholog_group_id");
                String groupName = rsGroup.getString("name");
                createLayout(groupId, groupName, taxons, rbhEdges, svgTemplate,
                        psSequence, psSimilarity, psUpdateImage);

                groupCount++;
                if (groupCount % 100 == 0) {
                    // psUpdateImage.executeBatch();
                    logger.debug(groupCount + " groups created...");
                }
            }
            if (groupCount % 100 != 0) psUpdateImage.executeBatch();

            logger.info("Total " + groupCount + " groups created.");
            rsGroup.close();
            stGroup.close();
            psSimilarity.close();

            // create signal id finished
            if (!hasMore) signalFile.createNewFile();
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
        } catch (SecurityException ex) {
            throw new OrthoMCLException(ex);
        }
    }

    private String[] loadSVGTemplate(File svgFile) throws IOException {
        String[] template = new String[3];
        BufferedReader reader = new BufferedReader(new FileReader(svgFile));
        StringBuffer buffer = new StringBuffer();
        String line;

        // read header
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("$$DataSection$$")) break;
            buffer.append(line);
            buffer.append("\n");
        }
        template[0] = buffer.toString();

        // read middle part
        buffer = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.equals("$$DisplaySection$$")) break;
            buffer.append(line);
            buffer.append("\n");
        }
        template[1] = buffer.toString();

        // read footer
        buffer = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            buffer.append(line.trim());
            buffer.append("\n");
        }
        template[2] = buffer.toString();

        reader.close();
        return template;
    }

    private Map<OrthomclEdge, OrthomclEdge> loadRBHFile(File rbhFile)
            throws IOException {
        Map<OrthomclEdge, OrthomclEdge> edges = new HashMap<OrthomclEdge, OrthomclEdge>();
        BufferedReader reader = new BufferedReader(new FileReader(rbhFile));

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim().toLowerCase();
            if (line.length() == 0 || line.charAt(0) == '#') continue;

            String[] parts = line.split("\\s+");
            int queryId = Integer.parseInt(parts[0]);
            int subjectId = Integer.parseInt(parts[1]);
            OrthomclEdge edge = new OrthomclEdge(queryId, subjectId);

            int pos = parts[3].toLowerCase().indexOf('e');
            if (pos >= 0) {
                edge.PValueMant = Double.parseDouble(parts[3].substring(0, pos));
                edge.PValueExp = Integer.parseInt(parts[3].substring(pos + 1));
            } else {
                edge.PValueMant = Double.parseDouble(parts[3]);
                edge.PValueExp = 0;
            }

            if (parts[2].equalsIgnoreCase("o")) {
                edge.Type = EdgeType.BestHit;
            } else if (parts[2].equalsIgnoreCase("i")) {
                edge.Type = EdgeType.BetterHit;
            }
            if (!edges.containsKey(edge)) edges.put(edge, edge);
        }
        reader.close();
        return edges;
    }

    private Map<Integer, Taxon> loadTaxons() throws SQLException {
        Statement stTaxon = connection.createStatement();
        ResultSet rsTaxon = stTaxon.executeQuery("SELECT ot.taxon_id, "
                + "      ot.three_letter_abbrev, ot.name "
                + " FROM apidb.OrthomclTaxon ot " + " WHERE ot.is_species = 1");
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
            Map<Integer, Taxon> taxons,
            Map<OrthomclEdge, OrthomclEdge> rbhEdges, String[] svgTemplate,
            PreparedStatement psSequence, PreparedStatement psSimilarity,
            PreparedStatement psUpdateImage) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException, SQLException,
            IOException {
        // get a sequence-taxon map
        Map<Integer, Sequence> sequenceMap = getSequences(groupId, psSequence);
        int[] sequences = new int[sequenceMap.size()];
        int index = 0;
        for (int sequence : sequenceMap.keySet())
            sequences[index++] = sequence;

        // enumerate all pairs
        Map<OrthomclEdge, OrthomclEdge> edges = new HashMap<OrthomclEdge, OrthomclEdge>();
        int edgeIndex = 0;
        for (int i = 0; i < sequences.length - 1; i++) {
            for (int j = i + 1; j < sequences.length; j++) {
                int queryId = sequences[i];
                int subjectId = sequences[j];

                OrthomclEdge edge = rbhEdges.get(new OrthomclEdge(queryId,
                        subjectId));

                // try to extract general edge if it's not in RBH
                if (edge == null)
                    edge = extractEdge(queryId, subjectId, psSimilarity);

                if (edge != null) {
                    edge.EdgeId = edgeIndex++;
                    edges.put(edge, edge);
                }
            }
        }
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

    private Map<Integer, Sequence> getSequences(int groupId,
            PreparedStatement psSequence) throws SQLException {
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

    private OrthomclEdge extractEdge(int queryId, int subjectId,
            PreparedStatement psSimilarity) throws SQLException {
        // get all pairs
        psSimilarity.setInt(1, queryId);
        psSimilarity.setInt(2, subjectId);
        psSimilarity.setInt(3, subjectId);
        psSimilarity.setInt(4, queryId);
        ResultSet rsSimilarity = psSimilarity.executeQuery();
        int count = 0;
        double mantMax = 0;
        int expMax = Integer.MIN_VALUE;
        while (rsSimilarity.next()) {
            int pValueExp = rsSimilarity.getInt("pvalue_exp");
            if (expMax < pValueExp) {
                mantMax = rsSimilarity.getDouble("pvalue_mant");
                expMax = pValueExp;
            }
            count++;
        }
        rsSimilarity.close();

        // only output 2-way matches
        if (count != 2) return null;
        else {
            OrthomclEdge edge = new OrthomclEdge(queryId, subjectId);
            edge.PValueMant = mantMax;
            edge.PValueExp = expMax;
            return edge;
        }
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
