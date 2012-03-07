package org.orthomcl.client.biolayout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.swing.JApplet;

import org.orthomcl.common.BlastScore;
import org.orthomcl.common.EdgeType;
import org.orthomcl.common.Gene;
import org.orthomcl.common.Group;
import org.orthomcl.common.Organism;

public class BiolayoutApplet extends JApplet {

    /**
     * 
     */
    private static final long serialVersionUID = 7138224878833669667L;

    private static final String PARAM_GROUP_URL = "group-url";
    private static final String PARAM_ORGANISM_URL = "organism-url";
    private static final String PARAM_GROUP = "group";

    private final String groupUrl;
    private final String organismUrl;
    private final Map<Integer, Organism> organisms;
    private final BiolayoutPanel layoutPanel;

    public BiolayoutApplet() {
        URL codeBase = getCodeBase();
        groupUrl = codeBase.getHost() + getParameter(PARAM_GROUP_URL);
        organismUrl = codeBase.getHost() + getParameter(PARAM_ORGANISM_URL);
        organisms = new HashMap<Integer, Organism>();
        layoutPanel = new BiolayoutPanel();
    }

    @Override
    public void init() {
        super.init();
        initializeUI();

        try {
            loadOrganisms();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void start() {
        try {
            // load the default group
            String groupName = getParameter(PARAM_GROUP);
            loadGroup(groupName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void loadGroup(String groupName) throws IOException,
            DataFormatException {
        // reset previous group
        layoutPanel.setGroup(null);

        layoutPanel.setStatus("Loading group " + groupName + "...");
        Group group = readGroup(groupName);

        layoutPanel.setStatus(null);
        layoutPanel.setGroup(group);
    }

    private void initializeUI() {
        setContentPane(layoutPanel);
    }

    private void loadOrganisms() throws IOException {
        layoutPanel.setStatus("Loading organisms...");

        URL url = new URL(organismUrl);
        URLConnection connection = url.openConnection();
        connection.connect();
        DataInputStream input = new DataInputStream(connection.getInputStream());
        // read organism count;
        int count = input.readInt();
        for (int i = 0; i < count; i++) {
            int organismId = input.readInt();
            Organism organism = new Organism(organismId);
            byte nameLen = input.readByte();
            organism.setName(readString(input, nameLen));
            byte abbrevLen = input.readByte();
            organism.setAbbreviation(readString(input, abbrevLen));

            organisms.put(organismId, organism);
        }
        input.close();
    }

    private String readString(DataInputStream input, int length)
            throws IOException {
        byte[] buffer = new byte[length];
        input.read(buffer);
        return new String(buffer);
    }

    private Group readGroup(String groupName) throws IOException,
            DataFormatException {
        URL url = new URL(groupUrl + "&group=" + groupName);
        URLConnection connection = url.openConnection();
        connection.connect();
        DataInputStream input = new DataInputStream(connection.getInputStream());

        int groupId = input.readInt();
        Group group = new Group(groupId);
        group.setName(groupName);

        // get gene count & layout data length
        int geneCount = input.readInt();
        int layoutLength = input.readInt();
        if (layoutLength > 0) {
            byte[] buffer = new byte[layoutLength];
            input.read(buffer);
            parseLayout(group, buffer);
        }

        // get genes
        parseGenes(group, input, geneCount);

        return group;
    }

    private void parseLayout(Group group, byte[] data) throws IOException,
            DataFormatException {
        // uncompress the data
        Inflater decompressor = new Inflater();
        decompressor.setInput(data);
        ByteArrayOutputStream output = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!decompressor.finished()) {
            int count = decompressor.inflate(buffer);
            output.write(buffer, 0, count);
        }
        output.close();

        data = output.toByteArray();
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(
                data));
        // read gene count & edge count
        int geneCount = input.readInt();
        int edgeCount = input.readInt();

        // read genes
        for (int i = 0; i < geneCount; i++) {
            int geneId = input.readInt();
            Gene gene = new Gene(geneId);
            float x = input.readFloat();
            float y = input.readFloat();
            gene.getPoint().setLocation(x, y);
            group.getGenes().put(geneId, gene);
        }

        // read edges
        EdgeType[] types = EdgeType.values();
        for (int i = 0; i < edgeCount; i++) {
            int queryId = input.readInt();
            int subjectId = input.readInt();
            byte typeId = input.readByte();
            float evalueMant = input.readFloat();
            short evalueExp = input.readShort();

            BlastScore score = new BlastScore(group, queryId, subjectId);
            score.setType(types[typeId]);
            score.setEvalueMant(evalueMant);
            score.setEvalueExp(evalueExp);
        }
    }

    private void parseGenes(Group group, DataInputStream input, int geneCount)
            throws IOException {
        for (int i = 0; i < geneCount; i++) {
            int geneId = input.readInt();
            Gene gene = group.getGenes().get(geneId);
            gene.setOrganismId(input.readInt());
            gene.setLength(input.readInt());
            byte sourceIdLength = input.readByte();
            gene.setSourceId(readString(input, sourceIdLength));
            int descLength = input.readInt();
            if (descLength > 0)
                gene.setDescription(readString(input, descLength));
        }
    }
}
