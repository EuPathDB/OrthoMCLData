package OrthoMCLData::Load::Plugin::UpdateOrthologGroup;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::ApiDB::OrthologGroupAaSequence;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;

#use ApiCommonData::Load::Util;

my $argsDeclaration =
[
 stringArg({ descr => 'Suffix for SimilarSequences table',
	     name  => 'simSeqTableSuffix',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'Suffix for Ortholog, CoOrtholog, and InParalog tables',
	     name  => 'orthologTableSuffix',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'overwrite existing statistics or only add statistics where number_of_match_pairs is null',
	     name  => 'overwriteExisting',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'specify core (C), peripheral (P), and/or residual (R) groups',
	     name  => 'groupTypesCPR',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

];

my $purpose = <<PURPOSE;
update ApiDB::OrthologGroup and ApiDB::OrthologGroupAaSequence tables.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
update apidb.orthologgroup average percent identity,average percent match, average mantissa, average exponent, average connectivity, number match pairs and apidb.orthologgroupaasequence.connectivity.
PURPOSE_BRIEF

#Avg connectivity is the connectivity of each sequence divivided by the group size.  A sequence connectivity is the total number of seqs it has a similarity to.(NOTE: this should probably normalize by the size of the group, ie, divide each seqs score by N choose 2)

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup,
ApiDB.OrthologGroupAASequence
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthologGroup,
ApiDB.OrthologGroupAASequence
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, update should only affect rows that have not been updated.
RESTART

my $failureCases = <<FAIL_CASES;

FAIL_CASES

my $documentation = { purpose          => $purpose,
                      purposeBrief     => $purposeBrief,
                      notes            => $notes,
                      tablesAffected   => $tablesAffected,
                      tablesDependedOn => $tablesDependedOn,
                      howToRestart     => $howToRestart,
                      failureCases     => $failureCases };



# ----------------------------------------------------------------------

sub new {
  my ($class) = @_;
  my $self = {};
  bless($self,$class);

  $self->initialize({ requiredDbVersion => 4,
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
		      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}



# ======================================================================

sub run {
    my ($self) = @_;

    my $simSeqTableSuffix = $self->getArg('simSeqTableSuffix');
    my $orthologTableSuffix = $self->getArg('orthologTableSuffix');
    my $groupTypesCPR = uc($self->getArg('groupTypesCPR'));
    my $overwriteExisting = uc($self->getArg('overwriteExisting'));

    if ( $groupTypesCPR !~ /^[CPRcpr]{1,3}$/ ) {
	die "The orthoGroup type must consist of C, P, and/or R. The value is currently '$groupTypesCPR'\n";
    }
    if ( $overwriteExisting !~ /^[YyNn]/ ) {
	die "The overwriteExisting variable must start with Y, y, N or n. The value is currently '$overwriteExisting'\n";
    }

    if ( lc($orthologTableSuffix) eq "peripheral" ) {
	$orthologTableSuffix = "peripheral";
    }

    my $unfinished = $self->getUnfinishedOrthologGroups($groupTypesCPR,$overwriteExisting);

    my ($updatedGrps,$updatedgrpsAaSeqs) = $self->processUnfinishedGroups($unfinished, $simSeqTableSuffix, $orthologTableSuffix);

    $self->log("$updatedGrps apidb.OrthologGroups and $updatedgrpsAaSeqs apidb.OrthologGroupAaSequence rows updated\n");
}


sub getUnfinishedOrthologGroups {
  my ($self,$groupTypesCPR,$overwriteExisting) = @_;

  $self->log ("Getting the ids of groups not yet updated\n");

  my %types = map { $_ => 1 } split('',uc($groupTypesCPR));
  my $text = join("','",keys %types);
  $text = "('$text')";

  my $overwriteText="";
  if ( $overwriteExisting =~ /^[Nn]/ ) {
      $overwriteText = "AND number_of_match_pairs IS NULL";
  }

  my %unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT ortholog_group_id, core_peripheral_residual
     FROM apidb.OrthologGroup
     WHERE core_peripheral_residual in $text
	   AND number_of_members > 1
	   $overwriteText
EOF

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  while (my @row = $sth->fetchrow_array()) {
      $unfinished{$row[0]}=$row[1];
  }

  my $num = keys %unfinished;

  $self->log ("   There are $num unfinished groups\n");

  return \%unfinished;
}

sub processUnfinishedGroups {
  my ($self, $unfinished, $simSeqTableSuffix, $orthologTableSuffix) = @_;

  my $updatedGrps;

  my $updatedGrpsAaSeqs;

  my $dbh = $self->getQueryHandle();

  my $sqlSelectOrthGrpAASeq = <<"EOF";
     SELECT oga.aa_sequence_id, x.secondary_identifier
       FROM apidb.OrthologGroupAaSequence oga, dots.ExternalAASequence x
       WHERE oga.ortholog_group_id = ? and oga.aa_sequence_id = x.aa_sequence_id
EOF

  my $sth = $dbh->prepare($sqlSelectOrthGrpAASeq);

  my $sqlSelectSimSeqs = "
     SELECT s.evalue_mant, s.evalue_exp,
       s.percent_identity, s.percent_match
     FROM apidb.SimilarSequences$simSeqTableSuffix s
     WHERE (s.query_id = ? AND s.subject_id = ?)
            OR (s.subject_id = ? AND s.query_id = ?)
";

  my $sth2 = $dbh->prepare($sqlSelectSimSeqs);

  my $sth3="";
  if ($orthologTableSuffix ne "peripheral") {
      my $conCount = <<"EOF";
select count(*) from
     (SELECT sequence_id_a
      FROM apidb.ortholog$orthologTableSuffix
      where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? and sequence_id_b = ?)
      UNION
      SELECT sequence_id_a
      FROM apidb.coortholog$orthologTableSuffix
      where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? and sequence_id_b = ?)
      UNION
      SELECT sequence_id_a
      FROM apidb.inparalog$orthologTableSuffix
      where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? and sequence_id_b = ?))
EOF
      $sth3 = $dbh->prepare($conCount);
  }

  foreach my $groupId (keys %{$unfinished}) {
    $self->log("Processing group_id: $groupId\n");

    my @seqIdArr;

    $sth->execute($groupId);

    while (my @row = $sth->fetchrow_array()) {
      my $seqId = $row[0];
      my $sourceId = $row[1];
      push (@seqIdArr, "${seqId},$sourceId");
    }

    next if @seqIdArr < 2;

    my ($grps, $aaseqs) = $self->processSeqsInGroup(\@seqIdArr, $groupId,$sth2,$sth3,$orthologTableSuffix);

    $updatedGrps += $grps;

    $updatedGrpsAaSeqs += $aaseqs;

    $self->log("$updatedGrps OrthologGroup rows updated\n     $updatedGrpsAaSeqs OrthologGroupAaSequence rows updated\n") if ($updatedGrps % 1000 == 0);
  }

  return ($updatedGrps,$updatedGrpsAaSeqs);

}

sub processSeqsInGroup {
  my ($self,$seqIdArr, $groupId, $sth, $sth2, $orthologTableSuffix) = @_;

  my $grpSize = @{$seqIdArr};

  $self->log ("Processing $grpSize seqs in $groupId\n");

  my $numMatchPairs = 0;     # A<->B and B<->A will be counted as 1
  my $numOneWays = 0;      # A<->B and B<->A will be counted as 2
  my $sumPercentIdentity = 0;
  my $sumPercentMatch = 0;
  my @eValues;
  my %connectivity;

  for (my $i = 0; $i < $grpSize - 1; $i++) {
    for (my $j = $i + 1; $j < $grpSize ; $j++) {
      my @sequence1 = split (/,/, $seqIdArr->[$i]);
      my @sequence2 = split (/,/, $seqIdArr->[$j]);

      $sth->execute($sequence1[1], $sequence2[1], $sequence1[1], $sequence2[1]);

      my $isPair=0;
      while (my @row = $sth->fetchrow_array()) {
	  die "Cannot handle e-value of 0e0 for $sequence1[1] and $sequence2[1]. Need to set to lowest e-value minus 1." if ($row[0]==0 && $row[1]==0);
	  $row[0] = 1 if ($row[0] == 0);
	  my $eValue = $row[0] . "e" . $row[1];
	  push @eValues, log($eValue)/log(10);
	  $sumPercentMatch += $row[3];
	  $sumPercentIdentity += $row[2];
	  $numOneWays++;
	  $isPair=1 if ($eValue <= 1e-5);
      }
      $numMatchPairs++ if ($isPair==1);
      
      if ($orthologTableSuffix ne "peripheral") {
	  my $isConnected = $self->getPairIsConnected($sequence1[1], $sequence2[1], $sth2);
	  $connectivity{$seqIdArr->[$i]} += $isConnected;
	  $connectivity{$seqIdArr->[$j]} += $isConnected;
      }
    }
  }
  my $grpAaSeqUpdated += $self->updateOrthologGroupAaSequences($groupId, $seqIdArr, \%connectivity, $orthologTableSuffix);

  my $groupsUpdated += $self->updateOrthologGroup($groupId,$numOneWays,$numMatchPairs,$sumPercentIdentity,$sumPercentMatch,\@eValues,\%connectivity,$grpSize,$orthologTableSuffix);

  return ($groupsUpdated,$grpAaSeqUpdated);

}

sub getPairIsConnected {
  my ($self,$seq1,$seq2,$sth) = @_;

  $sth ->execute($seq1,$seq2,$seq2,$seq1,$seq1,$seq2,$seq2,$seq1,$seq1,$seq2,$seq2,$seq1);

  my @row = $sth->fetchrow_array();

  return $row[0];
}

sub updateOrthologGroup {
  my ($self,$groupId,$numOneWays,$numMatchPairs,$sumPercentIdentity,$sumPercentMatch,$eValues,$connectivity,$grpSize,$orthologTableSuffix) = @_;

  $self->log ("Updating row for ortholog group_id $groupId\n");

  my $numOneWaysNoZero = ($numOneWays == 0) ? 1 : $numOneWays;

  my $avgPercentIdentity = sprintf("%.1f", $sumPercentIdentity/$numOneWaysNoZero);

  my $avgPercentMatch = sprintf("%.1f", $sumPercentMatch/$numOneWaysNoZero);

  my $medianEvalue = median($eValues);
  $medianEvalue = sprintf("%1.1e",10**$medianEvalue);
  my ($medianMant,$medianExp) = split(/e/,$medianEvalue);
  $medianExp = int($medianExp);

  my $maxPossiblePairs = ($grpSize * ($grpSize -1)) / 2;
  my $percentMatchPairs = sprintf("%.1f",100 * $numMatchPairs / $maxPossiblePairs);

  my $orthologGroup = GUS::Model::ApiDB::OrthologGroup->new({'ortholog_group_id'=>$groupId});
  $orthologGroup->retrieveFromDB();

  if ($orthologGroup->get('avg_percent_identity') != $avgPercentIdentity) {
    $orthologGroup->set('avg_percent_identity', $avgPercentIdentity);
  }

  if ($orthologGroup->get('avg_percent_match') != $avgPercentMatch) {
    $orthologGroup->set('avg_percent_match', $avgPercentMatch);
  }

  if ($orthologTableSuffix ne "peripheral") {
      my $avgConnectivity = sprintf("%.1f", $self->getAvgConnectivity($connectivity,$grpSize));
      if ($orthologGroup->get('avg_connectivity') != $avgConnectivity) {
	  $orthologGroup->set('avg_connectivity', $avgConnectivity);
      }
  }

  if ($orthologGroup->get('number_of_match_pairs') != $numMatchPairs) {
    $orthologGroup->set('number_of_match_pairs', $numMatchPairs);
  }

  if ($orthologGroup->get('percent_match_pairs') != $percentMatchPairs) {
    $orthologGroup->set('percent_match_pairs', $percentMatchPairs );
  }

  if ($orthologGroup->get('avg_evalue_exp') != $medianExp) {
    $orthologGroup->set('avg_evalue_exp', $medianExp);
  }

  if ($orthologGroup->get('avg_evalue_mant') != $medianMant) {
    $orthologGroup->set('avg_evalue_mant', $medianMant);
  }

  my $submit = $orthologGroup->submit();

  $self->undefPointerCache();

  return $submit;
}


sub getAvgConnectivity {
  my ($self,$connectivity,$grpSize) = @_;

  my $totalConnectivity = 0;

  foreach my $aaId (keys %{$connectivity}) {
    $totalConnectivity += $connectivity->{$aaId};
  }

  my $avgConnectivity = $totalConnectivity / $grpSize;

  return $avgConnectivity;
}

sub updateOrthologGroupAaSequences {
  my ($self, $groupId, $seqIdArr, $connectivity, $orthologTableSuffix) = @_;

  $self->log ("Updating orthologgroupaasequence rows\n");

  my $submitted;

  foreach my $idents (@{$seqIdArr}) {

    my @ids = split (/,/, $idents);

    my $orthGrpAaSeq = GUS::Model::ApiDB::OrthologGroupAaSequence->new({'aa_sequence_id'=>$ids[0],'ortholog_group_id'=>$groupId});

    my $groupExists = $orthGrpAaSeq->retrieveFromDB();

    if (! $groupExists) {
	die "Trying to get only one row for 'aa_sequence_id' $ids[0] and 'ortholog_group_id' $groupId but failed.\n";
    }

    if ($orthologTableSuffix ne "peripheral") {
	if ($orthGrpAaSeq->get('connectivity') != $connectivity->{$idents}) {
	    $orthGrpAaSeq->set('connectivity', $connectivity->{$idents});
	}
    }

    $submitted = $orthGrpAaSeq->submit();

    $self->undefPointerCache();
  }

  return $submitted;
}

sub median {
    my ($values) = @_;
    my @sortedValues = sort {$a <=> $b} @{$values};
    my $len = scalar @sortedValues;
    if ($len%2) { #odd
        return $sortedValues[int($len/2)];
    } else { #even
        return ($sortedValues[int($len/2)-1] + $sortedValues[int($len/2)])/2;
    }
}

# ----------------------------------------------------------------------


sub undoTables {
  my ($self) = @_;

  return (
         );
}

sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});

    my $groupTypesCPR = "";
    my $sql = " 
SELECT ap.string_value
FROM CORE.ALGORITHMPARAM ap, core.algorithmparamkey apk
WHERE ap.ALGORITHM_PARAM_KEY_ID = apk.ALGORITHM_PARAM_KEY_ID
      AND ap.ROW_ALG_INVOCATION_ID IN ($rowAlgInvocations)
      AND apk.ALGORITHM_PARAM_KEY = 'groupTypesCPR'";
    my $sh = $dbh->prepareAndExecute($sql);
    while (my @row = $sh->fetchrow_array()) {
	die "The groupTypesCPR value must consist of C, P and R only" if ($row[0] !~ /^[CPR]{1,3}$/);
	die "There are multiple groupTypesCPR values for this step" if ($groupTypesCPR ne "" && $groupTypesCPR ne $row[0]);
	$groupTypesCPR = $row[0];
    }
    $sh->finish();

    my %types = map { $_ => 1 } split('',$groupTypesCPR);
    my $text = join("','",keys %types);
    $text = "('$text')";

    $sql = "
UPDATE apidb.OrthologGroup
SET avg_percent_identity = NULL,
    avg_percent_match = NULL,
    avg_evalue_mant = NULL,
    avg_evalue_exp = NULL,
    avg_connectivity = NULL,
    number_of_match_pairs = NULL,
    percent_match_pairs = NULL
WHERE core_peripheral_residual in $text";    
    $sh = $dbh->prepareAndExecute($sql);
    $sh->finish();
}


1;
