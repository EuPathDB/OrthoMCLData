package org.orthomcl.data.common.layout.load.mapper;

import java.util.List;

import org.orthomcl.data.common.layout.core.BlastScore;
import org.orthomcl.data.common.layout.core.Gene;
import org.orthomcl.data.common.layout.core.GenePair;
import org.orthomcl.data.common.layout.core.Group;

public interface GeneMapper {

  List<Gene> selectGenes(Group group);

  List<BlastScore> selectBlastScores(Group group);
  
  List<BlastScore> selectBlastScoresEx(Group group);

  List<GenePair> selectOrthologs(Group group);

  List<GenePair> selectCoorthologs(Group group);

  List<GenePair> selectInparalogs(Group group);

  List<GenePair> selectPeripheralCore(Group group);

  List<GenePair> selectPeripheralPeripheral(Group group);

}
