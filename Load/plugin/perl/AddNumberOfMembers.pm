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

#use ApiCommonData::Load::Util;

my $argsDeclaration =
[
 stringArg({ descr => 'OrthoGroup types to edit (P=Peripheral,C=Core,R=Residual)',
	     name  => 'groupTypesCPR',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
];

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

    my $groupTypesCPR = uc($self->getArg('groupTypesCPR'));

    if ( $groupTypesCPR !~ /^[CPRcpr]{1,3}$/ ) {
	die "The orthoGroup type must consist of C, P, and/or R. The value is currently '$groupTypesCPR'\n";
    }

    my $unfinished = $self->getUnfinishedOrthologGroups($groupTypesCPR);

    my ($numUpdatedGroups) = $self->processUnfinishedGroups($unfinished);

    $self->log("$numUpdatedGroups apidb.OrthologGroups rows updated\n");
}


sub getUnfinishedOrthologGroups {
  my ($self,$groupTypesCPR) = @_;

  $self->log ("Getting the ids of groups where number_of_members = 0\n");

  my %types = map { $_ => 1 } split('',uc($groupTypesCPR));
  my $text = join("','",keys %types);
  $text = "('$text')";

  my %unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT ortholog_group_id, core_peripheral_residual
     FROM apidb.OrthologGroup
     WHERE number_of_members = 0
           AND core_peripheral_residual in $text
EOF

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  while (my @row = $sth->fetchrow_array()) {
      $unfinished{$row[0]}=$row[1];
  }

  my $num = keys %unfinished;

  $self->log ("   There are $num groups where number_of_members = 0\n");

  return \%unfinished;
}

sub processUnfinishedGroups {
  my ($self, $unfinished) = @_;

  my $numUpdatedGroups=0;

  my $dbh = $self->getQueryHandle();

  my $sqlNumProteinsInGroup = <<"EOF";
     SELECT count(*)
     FROM apidb.OrthologGroupAaSequence
     WHERE ortholog_group_id = ?
EOF

  my $sth = $dbh->prepare($sqlNumProteinsInGroup);

  $self->log("Calculating number_of_members for group_ids: ");
  foreach my $groupId (keys %{$unfinished}) {
    $self->log("$groupId ");

    $sth->execute($groupId);
    my ($numMembers) = $sth->fetchrow_array();

    if ($numMembers == 0) {
	die "No proteins were found in group with id '$groupId'\n";
    }

    my $orthologGroup = GUS::Model::ApiDB::OrthologGroup->new({'ortholog_group_id'=>$groupId});
    $orthologGroup->retrieveFromDB();
    $orthologGroup->set('number_of_members', $numMembers);
    my $submit = $orthologGroup->submit();
    $self->undefPointerCache();

    $numUpdatedGroups++;
  }

  return $numUpdatedGroups;
}




# ----------------------------------------------------------------------

sub undoUpdateTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroup',
	 );
}


sub undoTables {
  my ($self) = @_;

  return (
         );
}


1;
