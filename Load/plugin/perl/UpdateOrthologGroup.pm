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
[];

my $purpose = <<PURPOSE;
update ApiDB::OrthologGroup and ApiDB::OrthologGroupAaSequence tablesxs.
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

  $self->initialize({ requiredDbVersion => 3.6,
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
		      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}



# ======================================================================

sub run {
    my ($self) = @_;

    my $unfinished = $self->getUnfinishedOrthologGroups();

    my ($updatedGrps,$updatedgrpsAaSeqs) = $self->processUnfinishedGroups($unfinished);

    $self->log("$updatedGrps apidb.OrthologGroups and $updatedgrpsAaSeqs apidb.OrthologGroupAaSequence rows updated\n");
}


sub getUnfinishedOrthologGroups {
  my ($self) = @_;

  $self->log ("Getting the ids of groups not yet updated\n");

  my @unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT
       ortholog_group_id
     FROM apidb.OrthologGroup
     WHERE number_of_match_pairs IS NULL
EOF

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  while (my @row = $sth->fetchrow_array()) {
    push (@unfinished, $row[0]);
  }

  my $num = scalar @unfinished;

  $self->log ("   There are $num unfinished groups\n");

  return \@unfinished;
}

sub processUnfinishedGroups {
  my ($self, $unfinished) = @_;

  my $updatedGrps;

  my $updatedGrpsAaSeqs;

  my $dbh = $self->getQueryHandle();

  my $sqlSelectOrthGrpAASeq = <<"EOF";
     SELECT
       oga.aa_sequence_id, x.secondary_identifier
       FROM apidb.OrthologGroupAaSequence oga, dots.ExternalAASequence x
       WHERE oga.ortholog_group_id = ? and oga.aa_sequence_id = x.aa_sequence_id
EOF

  my $sth = $dbh->prepare($sqlSelectOrthGrpAASeq);

  my $sqlSelectSimSeqs = "
     SELECT
       s.evalue_mant, s.evalue_exp,
       s.percent_identity, s.percent_match
     FROM apidb.SimilarSequences s
     WHERE (s.query_id = ? AND s.subject_id = ?)
            OR (s.subject_id = ? AND s.query_id = ?)
";

  my $sth2 = $dbh->prepare($sqlSelectSimSeqs);

  my $conCount = <<"EOF";
     select count(*) from
     (SELECT sequence_id_a FROM apidb.ortholog where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? 
and sequence_id_b = ?)
     UNION
     SELECT sequence_id_a FROM apidb.coortholog where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? and sequence_id_b = ?)
     UNION
     SELECT sequence_id_a FROM apidb.inparalog where (sequence_id_a = ? and sequence_id_b = ?) or (sequence_id_a = ? and sequence_id_b = ?))
EOF

  my $sth3 = $dbh->prepare($conCount);


  foreach my $groupId (@{$unfinished}) {
    $self->log ("Processing group_id: $groupId\n");

    my @seqIdArr;

    $sth->execute($groupId);

    while (my @row = $sth->fetchrow_array()) {
      my $seqId = $row[0];
      my $sourceId = $row[1];
      push (@seqIdArr, "${seqId},$sourceId");
    }
    my ($grps, $aaseqs) = $self->processSeqsInGroup(\@seqIdArr, $groupId,$sth2,$sth3);

    $updatedGrps += $grps;

    $updatedGrpsAaSeqs += $aaseqs;

    $self->log("$updatedGrps OrthologGroup rows updated\n     $updatedGrpsAaSeqs OrthologGroupAaSequence rows updated\n") if ($updatedGrps % 1000 == 0);
  }

  return ($updatedGrps,$updatedGrpsAaSeqs);

}

sub processSeqsInGroup {
  my ($self,$seqIdArr, $groupId, $sth, $sth2) = @_;

  my $num = @{$seqIdArr};

  $self->log ("Processing $num seqs in $groupId\n");

  my $similarityCount = 0;
  my $sumPercentIdentity = 0;
  my $sumPercentMatch = 0;
  my $sumEvalue = 0;
  my %connectivity;
  my %lengthHsh;

  my $grpSize = @{$seqIdArr};

  for (my $i = 0; $i < $grpSize - 1; $i++) {
    for (my $j = $i + 1; $j < $grpSize ; $j++) {
      my @sequence1 = split (/,/, $seqIdArr->[$i]);

      my @sequence2 = split (/,/, $seqIdArr->[$j]);

      $sth->execute($sequence1[1], $sequence2[1], $sequence1[1], $sequence2[1]);

      while (my @row = $sth->fetchrow_array()) {
	$similarityCount++;
	$sumPercentMatch += $row[3];
	$sumPercentIdentity += $row[2];
	$sumEvalue +=  $row[0] . "e" . $row[1];
      }


      my $isConnected = $self->getPairIsConnected($sequence1[1], $sequence2[1], $sth2);
      $connectivity{$seqIdArr->[$i]} += $isConnected;
      $connectivity{$seqIdArr->[$j]} += $isConnected;

    }
  }
  my $grpAaSeqUpdated += $self->updateOrthologGroupAaSequences($seqIdArr, \%connectivity);

  my $groupsUpdated += $self->updateOrthologGroup($groupId, $similarityCount, $sumPercentIdentity, $sumPercentMatch, $sumEvalue, \%connectivity, $grpSize);

  return ($groupsUpdated,$grpAaSeqUpdated);

}

sub getPairIsConnected {
  my ($self,$seq1,$seq2,$sth) = @_;

  $sth ->execute($seq1,$seq2,$seq2,$seq1,$seq1,$seq2,$seq2,$seq1,$seq1,$seq2,$seq2,$seq1);

  my @row = $sth->fetchrow_array();

  return $row[0];
}

sub updateOrthologGroup {
  my ($self, $groupId, $similarityCount, $sumPercentIdentity, $sumPercentMatch, $sumEvalue, $connectivity, $grpSize) = @_;

  $self->log ("Updating row for ortholog group_id $groupId\n");

  my $avgPercentIdentity = sprintf("%.1f", $sumPercentIdentity/$similarityCount);

  my $avgPercentMatch = sprintf("%.1f", $sumPercentMatch/$similarityCount);

  my $avgConnectivity = sprintf("%.1f", $self->getAvgConnectivity($connectivity,$grpSize));

  my $avgEvalue = $sumEvalue/$similarityCount;

  my $orthologGroup = GUS::Model::ApiDB::OrthologGroup->new({'ortholog_group_id'=>$groupId});

  my $fixedAvgEValue = sprintf("%e",$avgEvalue);

  my($avgMant,$avgExp) = split(/e/,$fixedAvgEValue);

  my $numMatchPairs = $similarityCount / 2;

  $orthologGroup->retrieveFromDB();

  if ($orthologGroup->get('avg_percent_identity') != $avgPercentIdentity) {
    $orthologGroup->set('avg_percent_identity', $avgPercentIdentity);
  }

  if ($orthologGroup->get('avg_percent_match') != $avgPercentMatch) {
    $orthologGroup->set('avg_percent_match', $avgPercentMatch);
  }

  if ($orthologGroup->get('avg_connectivity') != $avgConnectivity) {
    $orthologGroup->set('avg_connectivity', $avgConnectivity);
  }

  if ($orthologGroup->get('number_of_match_pairs') != $numMatchPairs) {
    $orthologGroup->set('number_of_match_pairs', $numMatchPairs);
  }

  if ($orthologGroup->get('percent_match_pairs') != 100 * $numMatchPairs /($grpSize * ($grpSize -1)) ) {
    $orthologGroup->set('percent_match_pairs', 100 * $numMatchPairs /($grpSize * ($grpSize -1)) );
  }

  if ($orthologGroup->get('avg_evalue_exp') != $avgExp) {
    $orthologGroup->set('avg_evalue_exp', $avgExp);
  }

  if ($orthologGroup->get('avg_evalue_mant') != $avgMant) {
    $orthologGroup->set('avg_evalue_mant', $avgMant);
  }

  my $submit = $orthologGroup->submit();

  $self->undefPointerCache();

  return $submit;
}


sub getAvgConnectivity {
  my ($self,$connectivity,$grpSize) =@_;

  my $totalConnectivity = 0;

  foreach my $aaId (keys %{$connectivity}) {
    $totalConnectivity += $connectivity ->{$aaId};
  }

  my $avgConnectivity = $totalConnectivity / $grpSize;

  my $percentAvgConnectivity = ($avgConnectivity / ($grpSize - 1)) * 100; 

  #####return $avgConnectivity;
  return $percentAvgConnectivity;
}

sub updateOrthologGroupAaSequences {
  my ($self, $seqIdArr, $connectivity) = @_;

  $self->log ("Updating orthologgroupaasequence rows\n");

  my $submitted;

  foreach my $idents (@{$seqIdArr}) {

    my @ids = split (/,/, $idents);

    my $orthGrpAaSeq = GUS::Model::ApiDB::OrthologGroupAaSequence->new({'aa_sequence_id'=>$ids[0]});

    $orthGrpAaSeq->retrieveFromDB();

    if ($orthGrpAaSeq->get('connectivity') != $connectivity->{$ids[0]}) {
      $orthGrpAaSeq->set('connectivity', $connectivity->{$ids[0]});
    }

    $submitted = $orthGrpAaSeq->submit();

    $self->undefPointerCache();
  }

  return $submitted;
}



# ----------------------------------------------------------------------

sub undoUpdateTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence',
          'ApiDB.OrthologGroup',
	 );
}


sub undoTables {
  my ($self) = @_;

  return (
         );
}


1;
