package OrthoMCLData::Load::Plugin::AddNumberOfMembers;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::ApiDB::OrthologGroupAaSequence;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;

my $argsDeclaration = [];

my $purpose = <<PURPOSE;
update ApiDB::OrthologGroup table.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
update apidb.orthologgroup number_of_members.
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup
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

    my $unfinished = $self->getUnfinishedOrthologGroups();

    my ($numUpdatedGroups) = $self->processUnfinishedGroups($unfinished);

    $self->log("$numUpdatedGroups apidb.OrthologGroups rows updated\n");
}


sub getUnfinishedOrthologGroups {
  my ($self) = @_;

  $self->log ("Getting the ids of groups to add number_of_members\n");

  my @unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT group_id
     FROM apidb.OrthologGroup
EOF

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  while (my @row = $sth->fetchrow_array()) {
      push(@unfinished, $row[0]);
  }

  my $num = scalar @unfinished;

  $self->log ("   There are $num groups\n");

  return \@unfinished;
}

sub processUnfinishedGroups {
  my ($self, $unfinished) = @_;

  my $numUpdatedGroups=0;

  my $dbh = $self->getQueryHandle();

  my $sqlNumProteinsInGroup = <<"EOF";
  SELECT COUNT(aa_sequence_id)
  FROM apidb.orthologgroupaasequence
  WHERE group_id = ? 
EOF

  my $sqlCoreNumProteinsInGroup = <<"EOF";
  SELECT COUNT(aa_sequence_id) 
  FROM (SELECT ogs.aa_sequence_id, ogs.group_id, oas.is_core 
        FROM apidb.orthologgroupaasequence ogs, dots.orthoaasequence oas 
        WHERE ogs.group_id = ? 
        AND ogs.aa_sequence_id = oas.aa_sequence_id
       ) 
  WHERE is_core = 1
EOF

  my $sqlPeripheralNumProteinsInGroup = <<"EOF";
  SELECT COUNT(aa_sequence_id) 
  FROM (SELECT ogs.aa_sequence_id, ogs.group_id, oas.is_core 
        FROM apidb.orthologgroupaasequence ogs, dots.orthoaasequence oas 
        WHERE ogs.group_id = ? 
        AND ogs.aa_sequence_id = oas.aa_sequence_id
       ) 
  WHERE is_core = 0
EOF

  $self->log("Calculating core_number_of_members for group_ids: ");
  foreach my $groupId (@{$unfinished}) {
    $self->log("$groupId ");

    my $totalQry = $dbh->prepare($sqlNumProteinsInGroup);
    $totalQry->execute($groupId);
    my ($numMembers) = $totalQry->fetchrow_array();

    if ($numMembers == 0) {
	die "No proteins were found in group with id '$groupId'\n";
    }

    my $coreQry = $dbh->prepare($sqlCoreNumProteinsInGroup);
    $coreQry->execute($groupId);
    
    my ($coreNumMembers) = $coreQry->fetchrow_array();

    my $peripheralQry = $dbh->prepare($sqlPeripheralNumProteinsInGroup);
    $peripheralQry->execute($groupId);
    
    my ($peripheralNumMembers) = $peripheralQry->fetchrow_array();

    if ($coreNumMembers + $peripheralNumMembers != $numMembers) {
	die "Number of core and peripheral members do not add up to total number of members for group '$groupId'\n";
    }

    my $orthologGroup = GUS::Model::ApiDB::OrthologGroup->new({'group_id'=>$groupId});
    $orthologGroup->retrieveFromDB();
    $orthologGroup->set('number_of_members', $numMembers);
    $orthologGroup->set('number_of_core_members', $coreNumMembers);
    $orthologGroup->set('number_of_peripheral_members', $peripheralNumMembers);
    my $submit = $orthologGroup->submit();
    $self->undefPointerCache();

    $numUpdatedGroups++;
  }

  return $numUpdatedGroups;
}

# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;
  return ();
}

sub undoPreprocess {
    my ($self, $dbh) = @_;

    my $sql = "UPDATE apidb.OrthologGroup SET number_of_members = -1";
    my $sql = "UPDATE apidb.OrthologGroup SET number_of_core_members = -1";
    my $sql = "UPDATE apidb.OrthologGroup SET number_of_peripheral_members = -1";

    my $sh = $dbh->prepareAndExecute($sql);
    $sh->finish();
}


1;
