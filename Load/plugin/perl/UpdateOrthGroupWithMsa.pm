package OrthoMCLData::Load::Plugin::UpdateOrthGroupWithMsa;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;

use GUS::Model::ApiDB::OrthologGroup;

my $argsDeclaration =
[
  stringArg({name          => 'msaDir',
            descr          => 'directory containing msa files for each ortholog group with 2-100 member sequences',
            reqd           => 1,
            mustExist      => 1,
            constraintFunc => undef,
            isList         => 0, }),


 stringArg({ descr => 'regex used to select the ortholog group id from the name of the file, e.g. (OG30_12).msa where OG30_12 is the ortholog_group_id',
	     name  => 'fileRegex',
	     isList    => 0,
	     reqd  => 1,
	     mustExist      => 1,
	     constraintFunc => undef,
	   }),
];

my $purpose = <<PURPOSE;
update ApiDB::OrthologGroup multiple_sequence_alignment column.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
update apidb.orthologgroup, put contents of msa file into multiple_sequence_alignment.
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthologGroup,
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, plugin will only update rows with null multiple_sequence_alignment.
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

    my ($updatedGrps) = $self->loadMsaResults($unfinished);

    $self->log("$updatedGrps apidb.OrthologGroups rows updated with msa\n");
}


sub getUnfinishedOrthologGroups {
  my ($self) = @_;

  $self->log ("Getting the ids of groups not yet updated\n");

  my %unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT
       ortholog_group_id,name
     FROM apidb.OrthologGroup
     WHERE multiple_sequence_alignment IS NULL 
     AND number_of_members > 1 
     AND number_of_members <= 100
EOF

  $self->log ("     SQL: $sqlGetUnfinishedGroups\n");

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  while (my @row = $sth->fetchrow_array()) {
    $unfinished{$row[1]}=$row[0];
  }

  my $num = scalar (keys %unfinished);

  $self->log ("     There are $num unfinished groups\n");

  return \%unfinished;
}

sub loadMsaResults {
  my ($self, $unfinished) = @_;

  my $msaDir = $self->getArg('msaDir');

  my $regex = $self->getArg('fileRegex');

  my $updatedGrps;

  opendir (DIR,$msaDir) || die "Can't open directory $msaDir\n";

  while (defined (my $file = readdir (DIR))) {
    next if ($file eq "." || $file eq "..");

    my $groupName;

    if ($file =~ /$regex/){
      $groupName = $1;

      $updatedGrps += $self->processFile("$msaDir/$file",$unfinished->{$groupName}) if ($unfinished->{$groupName} > 1);

      $self->log("$updatedGrps apidb.orthologgroup rows have been updated\n") if ($updatedGrps % 1000 ==0);
    }
    else {
      $self->log("$file does not contain an ortholog_group_id conforming to the supplied regex. Check file names and regex\n");
    }
  }

  return $updatedGrps;
}

sub processFile {
   my ($self, $file,$groupId) = @_;

   open(FILE,$file) || die "Can't open $file for reading\n";

   my $msa;

   while (<FILE>){
     $msa .= $_;
   }

   my $update = $self->updateOrthologGroup($msa,$groupId);

   return $update;
}


sub updateOrthologGroup {
  my ($self, $msa, $groupId) = @_;

  my $orthologGroup = GUS::Model::ApiDB::OrthologGroup-> new({'ortholog_group_id'=>$groupId});

  $orthologGroup->retrieveFromDB();

  $orthologGroup->set('multiple_sequence_alignment', $msa);

  my $submit = $orthologGroup->submit();

  $self->undefPointerCache();

  return $submit;
}



# ----------------------------------------------------------------------

sub undoUpdateTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroup',
	 );
}

1;
