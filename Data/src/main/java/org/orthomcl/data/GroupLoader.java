package org.orthomcl.data;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class GroupLoader {
    
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(GroupLoader.class);

    private final Connection connection;
    private final PreparedStatement psOrganism;
    private final PreparedStatement psGroup;
    private final PreparedStatement psGene;

    public GroupLoader(final Connection connection) throws SQLException {
        this.connection = connection;

        psOrganism = connection.prepareStatement("SELECT taxon_id, name, "
                + "      three_letter_abbrev "
                + " FROM ApiDB.OrthoMCLTaxon WHERE is_species = 1");
        psOrganism.setFetchSize(200);

        psGroup = connection.prepareStatement("SELECT ortholog_group_id, "
                + "      name, number_of_members, layout_content "
                + " FROM ApiDB.OrthologGroup WHERE name = ?");
        psGroup.setFetchSize(1);

        psGene = connection.prepareStatement("SELECT ogs.aa_sequence_id, "
                + "   eas.source_id, eas.taxon_id, eas.length, eas.description"
                + " FROM Dots.ExternalAASequence eas, "
                + "      ApiDB.OrthologGroupAASequence ogs "
                + " WHERE ogs.aa_sequence_id = eas.aa_sequence_id "
                + "   AND ogs.ortholog_group_id = ?");
        psGene.setFetchSize(500);
    }

    public void close() throws SQLException {
        psOrganism.close();
        psGroup.close();
        psGene.close();
        connection.close();
    }

    public byte[] getOrganismsData() throws SQLException, IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(150 * 6);
        DataOutputStream output = new DataOutputStream(buffer);
        ResultSet resultSet = null;
        
        // put a place holder for organism size
        output.writeInt(0);
        int count =0;
        try {
            resultSet = psOrganism.executeQuery();
            while (resultSet.next()) {
                int organismId = resultSet.getInt("taxon_id");
                byte[] name = resultSet.getString("name").getBytes();
                byte[] abbreviation = resultSet.getString("three_letter_abbrev").getBytes();
                output.writeInt(organismId); // +0x0000, 4B
                output.writeByte(name.length); // +0x0004, 1B
                output.write(name);
                output.writeByte(abbreviation.length); // +0x0005, 1B
                output.write(abbreviation);
                count++;
            }
        } finally {
            if (resultSet != null) resultSet.close();
        }
        output.flush();
        output.close();
        byte[] data = buffer.toByteArray();
        
        // output actual size
        buffer = new ByteArrayOutputStream(4);
        output = new DataOutputStream(buffer);
        output.writeInt(count);
        System.arraycopy(buffer.toByteArray(), 0, data, 0, 4);
        
        return data;
    }

    public byte[] getGroupData(String name) throws SQLException,
            OrthoMCLDataException, IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(150 * 6);
        DataOutputStream output = new DataOutputStream(buffer);
        ResultSet resultSet = null;
        try {
            psGroup.setString(1, name);
            resultSet = psGroup.executeQuery();
            if (!resultSet.next())
                throw new OrthoMCLDataException("The group '" + name
                        + "' doesn't exist.");

            int groupId = resultSet.getInt("ortholog_group_id");
            int geneCount = resultSet.getInt("number_of_members");
            output.writeInt(groupId);// 0x0000, 4B
            output.writeInt(geneCount);// 0x0004, 4B

            Blob blob = resultSet.getBlob("layout_content");
            if (blob != null) {
                byte[] data = blob.getBytes(1, (int) blob.length());
                output.writeInt(data.length); // 0x0008, 4B
                output.write(data); // 0x00012, layout data
            } else {
                output.writeInt(0); // 0x0008, 4B
            }

            // load genes
            loadGenes(groupId, output, geneCount); // gene data

            output.flush();
            output.close();
            return buffer.toByteArray();
        } finally {
            if (resultSet != null) resultSet.close();
        }
    }

    private void loadGenes(int groupId, DataOutputStream output, int geneCount)
            throws SQLException, IOException, OrthoMCLDataException {
        ResultSet resultSet = null;
        try {
            psGene.setInt(1, groupId);
            resultSet = psGene.executeQuery();
            int count = 0;
            while (resultSet.next()) {
                int geneId = resultSet.getInt("aa_sequence_id");
                byte[] sourceId = resultSet.getString("source_id").getBytes();
                int organismId = resultSet.getInt("taxon_id");
                int length = resultSet.getInt("length");
                String desc = resultSet.getString("description");
                byte[] description = (desc == null) ? new byte[0]
                        : desc.getBytes();

                output.writeInt(geneId); // +0x0000, 4B
                output.writeInt(organismId); // +0x0004, 4B
                output.writeInt(length); // +0x0008, 4B
                output.writeByte(sourceId.length); // 0x000B, 1B
                output.write(sourceId);
                output.writeInt(description.length); // 4B
                if (description.length > 0) output.write(description);
                count++;
            }
            // verify the gene count
            if (count != geneCount)
                throw new OrthoMCLDataException("Group #" + groupId
                        + ": The expected gene count " + geneCount
                        + " doesn't match the actual genes loaded. (" + count
                        + ").");
        } finally {
            if (resultSet != null) resultSet.close();
        }
    }

}
