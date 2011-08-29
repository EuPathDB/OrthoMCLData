/**
 * 
 */
package org.apidb.orthomcl.load.plugin.biolayout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import oracle.jdbc.OraclePreparedStatement;

import org.apache.log4j.Logger;

/**
 * @author xingao
 * 
 */
public class GroupLoader {

    private static Logger logger = Logger.getLogger(GroupLoader.class);

    private PreparedStatement psSequence;
    private PreparedStatement psOrtholog;
    private PreparedStatement psCoortholog;
    private PreparedStatement psInparalog;
    private PreparedStatement psSimilarity;

    public GroupLoader(Connection connection) throws ClassNotFoundException,
            SQLException {
        String withClause = "WITH ogs AS "
                + "(SELECT eas.secondary_identifier AS combine_id "
                + "FROM apidb.OrthologGroupAaSequence ogs, "
                + "  dots.ExternalAaSequence eas "
                + "WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                + "  AND ogs.ortholog_group_id = ?) ";
        String whereClause = "WHERE o.sequence_id_a = ogs1.combine_id "
                + "  AND o.sequence_id_b = ogs2.combine_id ";
        String selectClause = "SELECT o.sequence_id_a as query_id, "
                + "o.sequence_id_b as subject_id ";
        psSequence = connection.prepareStatement("SELECT eas.aa_sequence_id, "
                + "     eas.source_id, eas.taxon_id, eas.description, "
                + "     t.three_letter_abbrev, t.name "
                + "FROM apidb.OrthologGroupAaSequence ogs, "
                + "     dots.ExternalAaSequence eas, apidb.OrthomclTaxon t "
                + "WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                + "  AND eas.taxon_id = t.taxon_id "
                + "  AND ogs.ortholog_group_id = ? ");
        ((OraclePreparedStatement) psSequence).setRowPrefetch(1000);
        psOrtholog = connection.prepareStatement(withClause + selectClause
                + "FROM apidb.Ortholog o, ogs ogs1, ogs ogs2 " + whereClause);
        ((OraclePreparedStatement) psOrtholog).setRowPrefetch(5000);
        psCoortholog = connection.prepareStatement(withClause + selectClause
                + "FROM apidb.Coortholog o, ogs ogs1, ogs ogs2 " + whereClause);
        ((OraclePreparedStatement) psCoortholog).setRowPrefetch(5000);
        psInparalog = connection.prepareStatement(withClause + selectClause
                + "FROM apidb.Inparalog o, ogs ogs1, ogs ogs2 " + whereClause);
        ((OraclePreparedStatement) psInparalog).setRowPrefetch(5000);
        psSimilarity = connection.prepareStatement(withClause
                + "SELECT ogs1.combine_id AS query_id, "
                + "  ogs2.combine_id AS subject_id, "
                + "  o.evalue_mant, o.evalue_exp "
                + "FROM apidb.SimilarSequences o, ogs ogs1, ogs ogs2 "
                + "WHERE o.query_id = ogs1.combine_id "
                + "  AND o.subject_id = ogs2.combine_id "
                + "  AND o.query_id < o.subject_id ");
        ((OraclePreparedStatement) psSimilarity).setRowPrefetch(5000);
    }

    public void close() throws SQLException {
        psCoortholog.close();
        psInparalog.close();
        psOrtholog.close();
        psSequence.close();
        psSimilarity.close();
    }

    public Group getGroup(int groupId) throws SQLException {
        Group group = new Group();
        group.groupId = groupId;

        // logger.debug("Testing...");
        loadSequences(group);
        loadOrthologs(group);
        loadCoorthologs(group);
        loadInparalogs(group);
        loadNormalEdges(group);
        // logger.debug("group #" + groupId + " loaded.");
        return group;
    }

    private void loadSequences(Group group) throws SQLException {
        psSequence.setInt(1, group.groupId);
        ResultSet resultSet = psSequence.executeQuery();

        while (resultSet.next()) {
            Node node = new Node();
            node.sequenceId = resultSet.getInt("aa_sequence_id");
            node.sourceId = resultSet.getString("source_id");
            node.taxonId = resultSet.getInt("taxon_id");
            node.abbreviation = resultSet.getString("three_letter_abbrev");
            node.description = resultSet.getString("description");
            if (node.description != null) {
                node.description = node.description.replaceAll("\\s+", " ").trim();
                node.description = node.description.replaceAll("<", "&lt;");
                node.description = node.description.replaceAll(">", "&gt;");
                node.description = node.description.replaceAll("\"", "'");
            } else node.description = "";
            node.organism = resultSet.getString("name");
            group.nodes.put(node.getCombinedId(), node);
        }
    }

    private void loadOrthologs(Group group) throws SQLException {
        psOrtholog.setInt(1, group.groupId);
        ResultSet resultSet = psOrtholog.executeQuery();
        while (resultSet.next()) {
            Edge edge = new Edge();
            edge.queryId = resultSet.getString("query_id");
            edge.subjectId = resultSet.getString("subject_id");
            edge.type = EdgeType.Ortholog;
            group.edges.put(edge, edge);
        }
    }

    private void loadCoorthologs(Group group) throws SQLException {
        psCoortholog.setInt(1, group.groupId);
        ResultSet resultSet = psCoortholog.executeQuery();
        while (resultSet.next()) {
            Edge edge = new Edge();
            edge.queryId = resultSet.getString("query_id");
            edge.subjectId = resultSet.getString("subject_id");
            edge.type = EdgeType.Coortholog;
            group.edges.put(edge, edge);
        }
    }

    private void loadInparalogs(Group group) throws SQLException {
        psInparalog.setInt(1, group.groupId);
        ResultSet resultSet = psInparalog.executeQuery();
        while (resultSet.next()) {
            Edge edge = new Edge();
            edge.queryId = resultSet.getString("query_id");
            edge.subjectId = resultSet.getString("subject_id");
            edge.type = EdgeType.Inparalog;
            group.edges.put(edge, edge);
        }
    }

    private void loadNormalEdges(Group group) throws SQLException {
        psSimilarity.setInt(1, group.groupId);
        ResultSet resultSet = psSimilarity.executeQuery();
        while (resultSet.next()) {
            Edge edge = new Edge();
            edge.queryId = resultSet.getString("query_id");
            edge.subjectId = resultSet.getString("subject_id");
            if (group.edges.containsKey(edge)) {
                edge = group.edges.get(edge);
            } else {
                edge.type = EdgeType.Normal;
            }
            edge.evalueMant = resultSet.getFloat("evalue_mant");
            edge.evalueExp = resultSet.getInt("evalue_exp");
            if (Math.abs(edge.evalueMant) > 0.00001) {
                double score = Math.log10(edge.evalueMant) + edge.evalueExp;
                edge.weight = Math.max(0, -score);
            } else edge.weight = 181;
            group.edges.put(edge, edge);
        }
    }
}
