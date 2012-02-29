package org.orthomcl.data.layout;

import java.util.List;

public interface GroupMapper {

    int selectMinExp();
    
    List<Group> selectGroups(int maxMember);
    
    List<Integer> selectSequences(int groupId);
    
    List<BlastScore> selectBlastScores(int groupId);
    
    void insertLayout(Sequence sequence);
}
