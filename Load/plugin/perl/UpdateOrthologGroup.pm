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

use ApiCommonData::Load::Util;


my $argsDeclaration =
[
    fileArg({name           => 'orthoFile',
            descr          => 'Ortholog Data (ortho.mcl). OrthologGroupName(gene and taxon count) followed by a colon then the ids for the members of the group',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG2_1009: osa|ENS1222992 pfa|PF11_0844...',
            constraintFunc => undef,
            isList         => 0, }),


 stringArg({ descr => 'Name of the External Database',
	     name  => 'extDbName',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),


 stringArg({ descr => 'Version of the External Database Release',
	     name  => 'extDbVersion',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

];

my $purpose = <<PURPOSE;
Insert an ApiDB::OrthologGroup and its members from an orthomcl groups file.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Load an orthoMCL analysis result.
PURPOSE_BRIEF

my $notes = <<NOTES;
Need a script to create the mapping file.
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup,
ApiDB.OrthologGroupAASequence
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.TaxonName,
Sres.ExternalDatabase,
Sres.ExternalDatabaseRelease,
Sres.ExternalAASequence
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, since the same ortholog group from the same OrthoMCL analysis version will only be loaded once.
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

  $self->initialize({ requiredDbVersion => 3.5,
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
       aa_sequence_id
       FROM apidb.OrthologGroupAaSequence
       WHERE ortholog_group_id = ?
EOF

  my $sth = $dbh->prepare($sqlSelectOrthGrpAASeq);

  foreach my $groupId (@{$unfinished}) {

    my @seqIdArr;

    $sth->execute($groupId);

    while (my @row = $sth->fetchrow_array()) {
      my $seqId = $row[0];
      push (@seqIdArr, $seqId);
    }
    my ($grps, $aaseqs) = $self->processSeqsInGroup(\@seqIdArr, $groupId);

    $updatedGrps += $grps;

    $updatedGrpsAaSeqs += $aaseqs;

    $self->log("$updatedGrps OrthologGroup rows updated\n     $updatedGrpsAaSeqs OrthologGroupAaSequence rows updated\n") if ($updatedGrps % 1000 == 0);
  }

  return ($updatedGrps,$updatedGrpsAaSeqs);

}


sub processSeqsInGroup {
  my ($self,$seqIdArr, $groupId) = @_;

  my $pairCount = 0;
  my $sumPercentIdentity = 0;
  my $sumPercentMatch = 0;
  my $sumEvalue = 0;
  my %connectivity;

  my $grpSize = @{$seqIdArr};

  my $dbh = $self->getQueryHandle();

  my $sqlSelectSimSeqs = <<"EOF";
     SELECT
       s.query_id , s.subject_id, s.query_match_length, s.subject_match_length, s.evalue_mant, s.evalue_exp,
       s.percent_identity
     FROM apidb.SimilarSequences s
     WHERE (s.query_id = ? AND s.subject_id = ?)
            OR (s.subject_id = ? AND s.query_id = ?)
EOF

  my $sth = $dbh->prepare($sqlSelectSimSeqs);

  for (my $i = 0; $i < $grpSize - 1; $i++) {
    for (my $j = $i + $1; $j < $grpSize ; $j++) {
      my $sequence1 = $seqIdArr->[$i];

      my $sequence2 = $seqIdArr->[$j];

      $sth->execute($sequence1, $sequence2);

      while (my @row = $sth->fetchrow_array()) {
	$pairCount++;
	$sumPercentMatch += $self->getPercentMatch($row[0],$row[1],$row[2],$row[3]);
	$sumPercentIdentity += $row[7];
	$sumEvalue +=  $row[4] . "e" . $row[5];
	$connectivity{$seqIdArr->[$i]}++;
	$connectivity{$seqIdArr->[$j]}++;
      }
    }
  }

  my $groupsUpdated += $self->updateOrthologGroup($groupId, $pairCount, $sumPercentIdentity, $sumPercentMatch, $sumEvalue, \%connectivity, $grpSize);

  my $grpAaSeqUpdated += $self->updateOrthologGroupAaSequences($seqIdArr, \%connectivity);

  return ($groupsUpdated,$grpAaSeqUpdated); 

}

sub getPercentMatch {
  my ($self, $queryId, $subjectId, $queryMatchLength, $subjectMatchLength) = @_;

  my $id;
  my $matchLength;

  if ($queryMatchLength < $subjectMatchLength) {
    $id = $queryId;
    $matchLength = $queryMatchLength;
  }
  else {
    $id = $subjectId;
    $matchLength = $subjectMatchLength;
  }

  my $dbh = $self->getQueryHandle();

  my $sqlLengthSeq = <<"EOF";
  SELECT
    length
  FROM dots.aasequence
  WHERE aa_sequence_id = $id
EOF

  my $sth = $dbh->prepareAndExecute($sqlLengthSeq);

  my @row = $sth->fetchrow_array();

  $sth-finish();

  my $percentMatch = 100 * $matchLength/$row[0];

  return $percentMatch;
}

sub updateOrthologGroup {
  my ($self, $groupId, $pairCount, $sumPercentIdentity, $sumPercentMatch, $sumEvalue, $connectivity, $grpSize) = @_;

  my $avgPercentIdentity = $sumPercentIdentity/$pairCount;

  my $avgPercentMatch = $sumPercentMatch/$pairCount;

  my $avgConnectivity = $self->getAvgConnectivity($connectivity,$grpSize);

  my $avgEvalue = $sumEvalue/$pairCount;

  my $orthologGroup = GUS::Model::ApiDB::OrthologGroup->new({'group_id'=>$groupId});

  my($avgMant,$avgExp) = split(/e/,$sumEvalue);

  $orthologGroup->retrieveFromDB();

  if ($orthologGroup->get('average_percent_identity') != $avgPercentIdentity) {
    $orthologGroup->set('average_percent_identity', $avgPercentIdentity);
  }

  if ($orthologGroup->get('average_percent_match') != $avgPercentMatch) {
    $orthologGroup->set('average_percent_match', $avgPercentMatch);
  }

  if ($orthologGroup->get('avg_connectivity') != $avgConnectivity) {
    $orthologGroup->set('avg_connectivity', $avgConnectivity);
  }

  if ($orthologGroup->get('number_of_match_pairs') != $pairCount) {
    $orthologGroup->set('number_of_match_pairs', $pairCount);
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

  return $avgConnectivity;
}

sub updateOrthologGroupAaSequences {
  my ($self, $seqIdArr, $connectivity) = @_;

  my $submitted;

  foreach my $id (@{$seqIdArr}) {

    my $orthGrpAaSeq = GUS::Model::ApiDB::OrthologGroupAaSequence->new({'aa_sequence_id'=>$id});

    $orthGrpAaSeq->retrieveFromDB();

    if ($orthGrpAaSeq->get('connectivity') != $connectivity->{$id}) {
      $orthGrpAaSeq->set('connectivity', $connectivity->{$id});
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

1;
