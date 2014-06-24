package org.orthomcl.data.load.mapper;

import java.util.List;

import org.orthomcl.data.core.BlastScore;
import org.orthomcl.data.core.Gene;
import org.orthomcl.data.core.GenePair;
import org.orthomcl.data.core.Group;

public interface GeneMapper {

  List<Gene> selectGenes(Group group);

  List<BlastScore> selectBlastScores(Group group);

  List<GenePair> selectOrthologs(Group group);

  List<GenePair> selectCoorthologs(Group group);

  List<GenePair> selectInparalogs(Group group);
}
