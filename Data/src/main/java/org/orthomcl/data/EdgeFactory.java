package org.orthomcl.data;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.orthomcl.data.BlastScore;
import org.orthomcl.data.EdgeType;
import org.orthomcl.data.GenePair;
import org.orthomcl.data.Group;

public class EdgeFactory {

    private final Connection connection;
    private final PreparedStatement psBlast;
    private final PreparedStatement psOrtholog;
    private final PreparedStatement psCoortholog;
    private final PreparedStatement psInparalog;
    private final PreparedStatement psUpdate;

    public EdgeFactory(Connection connection) throws SQLException {
        this.connection = connection;

        psBlast = connection.prepareStatement("WITH Sequences AS ("
                + "   SELECT eas.secondary_identifier AS source_id, "
                + "          eas.aa_sequence_id "
                + "   FROM DoTS.ExternalAASequence eas, "
                + "        ApiDB.OrthologGroupAASequence ogs"
                + "   WHERE eas.aa_sequence_id = ogs.aa_sequence_id"
                + "     AND ogs.ortholog_group_id = ?) "
                + " SELECT s1.aa_sequence_id AS query_id, "
                + "        s2.aa_sequence_id AS subject_id, "
                + "        ss.evalue_mant, ss.evalue_exp "
                + " FROM ApiDB.SimilarSequences ss, Sequences s1, Sequences s2"
                + " WHERE ss.query_id = s1.source_id "
                + "   AND ss.subject_id = s2.source_id");
        psBlast.setFetchSize(5000);

        psOrtholog = connection.prepareStatement(makeTypeSql("Ortholog"));
        psOrtholog.setFetchSize(5000);

        psCoortholog = connection.prepareStatement(makeTypeSql("Coortholog"));
        psCoortholog.setFetchSize(5000);

        psInparalog = connection.prepareStatement(makeTypeSql("Inparalog"));
        psInparalog.setFetchSize(5000);

        psUpdate = connection.prepareStatement("UPDATE ApiDB.OrthologGroup "
                + " SET layout_content = ? WHERE ortholog_group_id = ? ");
    }

    public void close() throws SQLException {
        psBlast.close();
        psOrtholog.close();
        psCoortholog.close();
        psInparalog.close();
        psUpdate.close();
        connection.close();
    }

    private String makeTypeSql(String table) {
        StringBuilder sql = new StringBuilder("WITH Sequences AS (");
        sql.append("   SELECT eas.secondary_identifier AS source_id, ");
        sql.append("          eas.aa_sequence_id ");
        sql.append("   FROM DoTS.ExternalAASequence eas, ");
        sql.append("        ApiDB.OrthologGroupAASequence ogs");
        sql.append("   WHERE eas.aa_sequence_id = ogs.aa_sequence_id");
        sql.append("     AND ogs.ortholog_group_id = ?) ");
        sql.append(" SELECT s1.aa_sequence_id AS query_id, ");
        sql.append("        s2.aa_sequence_id AS subject_id ");
        sql.append(" FROM ApiDB." + table + " ss, Sequences s1, Sequences s2");
        sql.append(" WHERE ss.sequence_id_a = s1.source_id ");
        sql.append("   AND ss.sequence_id_b = s2.source_id");
        return sql.toString();
    }

    public void loadBlastScores(Group group, double maxWeight)
            throws SQLException {
        psBlast.setInt(1, group.getId());
        ResultSet resultSet = null;
        try {
            resultSet = psBlast.executeQuery();
            while (resultSet.next()) {
                int queryId = resultSet.getInt("query_id");
                int subjectId = resultSet.getInt("subject_id");
                float evalueMant = resultSet.getFloat("evalue_mant");
                int evalueExp = resultSet.getInt("evalue_exp");
                if (evalueMant < 0.0001) evalueMant = 1; // in the case of 0

                GenePair pair = new GenePair(queryId, subjectId);
                BlastScore edge = group.getScores().get(pair);
                if (edge != null) { // an inversed score exists, compute weight
                                    // as average of log(evalue).
                    double mant = Math.log10(evalueMant * edge.getEvalueMant());
                    double weight = maxWeight
                            + (mant + evalueExp + edge.getEvalueExp()) / 2;
                    edge.setWeight(weight);
                } else { // no inversed score, put the new edge in.
                    edge = new BlastScore(group, queryId, subjectId);
                    edge.setEvalueMant(evalueMant);
                    edge.setEvalueExp(evalueExp);
                    group.getScores().put(pair, edge);
                }
            }

            // compute weights for the single pairs
            for (BlastScore score : group.getScores().values()) {
                if (score.getWeight() < 0.001) {// weight hasn't be set.
                    double weight = maxWeight
                            + Math.log10(score.getEvalueMant())
                            + score.getEvalueExp();
                    score.setWeight(weight);
                }
            }
        } finally {
            if (resultSet != null) resultSet.close();
        }
    }

    public void loadEdgeTypes(Group group, EdgeType type) throws SQLException {
        PreparedStatement psType;
        if (type == EdgeType.Ortholog) psType = psOrtholog;
        else if (type == EdgeType.Coortholog) psType = psCoortholog;
        else if (type == EdgeType.Inparalog) psType = psInparalog;
        else return;

        psType.setInt(1, group.getId());
        ResultSet resultSet = null;
        try {
            resultSet = psType.executeQuery();
            while (resultSet.next()) {
                int queryId = resultSet.getInt("query_id");
                int subjectId = resultSet.getInt("subject_id");

                GenePair pair = new GenePair(queryId, subjectId);
                BlastScore edge = group.getScores().get(pair);
                edge.setType(type);
            }
        } finally {
            if (resultSet != null) resultSet.close();
        }
    }

    public void saveGroup(Group group, byte[] content) throws SQLException {
        ByteArrayInputStream input = new ByteArrayInputStream(content);
        psUpdate.setBinaryStream(1, input, content.length);
        psUpdate.setInt(2, group.getId());
        psUpdate.executeUpdate();
    }
}
