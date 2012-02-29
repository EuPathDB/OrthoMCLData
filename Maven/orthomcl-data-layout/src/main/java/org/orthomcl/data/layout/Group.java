package org.orthomcl.data.layout;

import java.util.HashMap;
import java.util.Map;

import net.lliira.common.graphics.layout.Network;
import net.lliira.common.graphics.layout.Node;

public class Group implements Network {

    private final int id;
    private final Map<Integer, Sequence> sequences;
    private final Map<Pair, BlastScore> blastScores;
    
    private int members;

    public Group(int id) {
        this.id = id;
        this.sequences = new HashMap<Integer, Sequence>();
        this.blastScores = new HashMap<Pair, BlastScore>();
    }

    public int getId() {
        return id;
    }

    public int getMembers() {
        return members;
    }

    public void setMembers(int members) {
        this.members = members;
    }

    public boolean connected(Node sequenceA, Node sequenceB) {
        int idA = ((Sequence) sequenceA).getId();
        int idB = ((Sequence) sequenceB).getId();
        return blastScores.containsKey(new Pair(idA, idB));
    }

    public BlastScore getBlastScore(Pair pair) {
        return blastScores.get(pair);
    }

    @Override
    public BlastScore[] getEdges() {
        return blastScores.values().toArray(new BlastScore[0]);
    }

    public void addBlastScore(Pair pair, BlastScore blastScore) {
        blastScores.put(pair, blastScore);
    }

    public Sequence getSequence(int sequenceId) {
        return sequences.get(sequenceId);
    }
    
    public int getSequenceCount() {
        return sequences.size();
    }

    @Override
    public Sequence[] getNodes() {
        return sequences.values().toArray(new Sequence[0]);
    }

    public void addSequence(Sequence sequence) {
        sequences.put(sequence.getId(), sequence);
    }
}
