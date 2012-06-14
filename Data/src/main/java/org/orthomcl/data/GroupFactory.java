package org.orthomcl.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import oracle.jdbc.internal.OraclePreparedStatement;

public class GroupFactory {

    private final Connection connection;
    private final PreparedStatement psMaxExp;
    private final PreparedStatement psGroupGene;

    public GroupFactory(Connection connection) throws SQLException {
        this.connection = connection;

        psMaxExp = connection.prepareStatement("SELECT evalue_exp "
                + " FROM ApiDB.SimilarSequences "
                + " WHERE evalue_mant = 0 AND rownum = 1");

        psGroupGene = connection.prepareStatement("SELECT ogs.aa_sequence_id, "
                + "      og.ortholog_group_id "
                + " FROM ApiDB.OrthologGroupAASequence ogs, "
                + "      ApiDB.OrthologGroup og "
                + " WHERE og.number_of_members <= ? "
                + "   AND og.layout_content IS NULL "
                + "   AND og.ortholog_group_id = ogs.ortholog_group_id "
                + " ORDER BY og.ortholog_group_id ASC");
        ((OraclePreparedStatement) psGroupGene).setRowPrefetch(5000);
    }

    public void close() throws SQLException {
        psMaxExp.close();
        psGroupGene.close();
        connection.close();
    }

    public int getMaxWeight() throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = psMaxExp.executeQuery();
            int exp = -180;
            if (resultSet.next()) exp = resultSet.getInt("evalue_exp");
            return -exp + 1;
        } finally {
            if (resultSet != null) resultSet.close();
        }
    }

    public ResultSet getGroupGenes(int maxMember) throws SQLException {
        psGroupGene.setInt(1, maxMember);
        return psGroupGene.executeQuery();
    }
}
