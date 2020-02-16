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

 stringArg({ descr => 'specify core (C), peripheral (P), and/or residual (R) groups',
	     name  => 'groupTypesCPR',
	     isList    => 0,
	     reqd  => 1,
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

    my $groupTypesCPR = uc($self->getArg('groupTypesCPR'));
    if ( $groupTypesCPR !~ /^[CPRcpr]{1,3}$/ ) {
	die "The orthoGroup type must consist of C, P, and/or R. The value is currently '$groupTypesCPR'\n";
    }

    my $unfinished = $self->getUnfinishedOrthologGroups($groupTypesCPR);

# Mark: need to edit this script, also proofread UpdateOrthologGroup.pm
    my ($updatedGrps) = $self->loadMsaResults($unfinished);

    $self->log("$updatedGrps apidb.OrthologGroups rows updated with msa\n");
}


sub getUnfinishedOrthologGroups {
  my ($self,$groupTypesCPR) = @_;

  $self->log ("Getting the ids of groups not yet updated\n");

  my %types = map { $_ => 1 } split('',uc($groupTypesCPR));
  my $text = join("','",keys %types);
  $text = "('$text')";

  my %unfinished;

  my $sqlGetUnfinishedGroups = <<"EOF";
     SELECT ortholog_group_id,name
     FROM apidb.OrthologGroup
     WHERE multiple_sequence_alignment IS NULL 
           AND core_peripheral_residual in $text
	   AND number_of_members > 1
EOF

  $self->log ("     SQL: $sqlGetUnfinishedGroups\n");

  my $dbh = $self->getQueryHandle();

  my $sth = $dbh->prepareAndExecute($sqlGetUnfinishedGroups);

  my $numGroups=0;
  while (my @row = $sth->fetchrow_array()) {
    if (exists $unfinished{$row[1]}) {
	push $unfinished{$row[1]}, $row[0];
    } else { 
	$unfinished{$row[1]} = [$row[0]];
    }
    $numGroups++;
  }

  $self->log ("     There are $numGroups unfinished groups\n");

  return \%unfinished;
}

sub loadMsaResults {
  my ($self, $unfinished) = @_;

  my $msaDir = $self->getArg('msaDir');
  my $regex = $self->getArg('fileRegex');
  my $updatedGrps=0;

  opendir (DIR,$msaDir) || die "Can't open directory $msaDir\n";
  while (defined (my $file = readdir (DIR))) {
    next if ($file eq "." || $file eq "..");
    my $groupName;
    if ($file =~ /$regex/) { 
      $groupName = $1;
      if (exists $unfinished->{$groupName}) {
	  foreach my $unfinishedGroupId (@{$unfinished->{$groupName}}) {
	      $updatedGrps += $self->processFile("$msaDir/$file",$unfinishedGroupId);
	  }
	  $self->log("$updatedGrps apidb.orthologgroup rows have been updated\n") if ($updatedGrps % 1000 ==0);
      }
    }
    else {
	$self->log("$file does not contain an ortholog group name conforming to the supplied regex. Check file names and regex\n");
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

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroup',
	 );
}

sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});

    my $sql = "UPDATE apidb.OrthologGroup
               SET multiple_sequence_alignment = NULL
               WHERE row_alg_invocation_id in ($rowAlgInvocations)";
    
    my $sh = $dbh->prepare($sql);
    $sh->execute();
    $sh->finish();
}

1;
