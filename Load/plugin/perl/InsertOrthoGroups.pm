package OrthoMCLData::Load::Plugin::InsertOrthoGroups;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;

my $argsDeclaration =
[
    fileArg({name           => 'orthoFile',
            descr          => 'Ortholog Data (ortho.mcl). OrthologGroupName(gene and taxon count) followed by a colon then the ids for the members of the group',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG2_1009: osa|ENS1222992 pfa|PF11_0844...',
            constraintFunc => undef,
            isList         => 0, }),

 stringArg({ descr => 'isResidual (0 or 1)',
	     name  => 'isResidual',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

stringArg({ descr => 'orthoVersion (7)',
	     name  => 'orthoVersion',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

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
Insert an ApiDB::OrthologGroup from an orthomcl groups file.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Load an orthoMCL group.
PURPOSE_BRIEF

my $notes = <<NOTES;
Need a script to create the mapping file.
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.ExternalDatabase,
Sres.ExternalDatabaseRelease
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

    my $orthologFile = $self->getArg('orthoFile');

    my $isResidual = $self->getArg('isResidual');
    die "The isResidual variable must be 1 or 0. It is currently set to '$isResidual'" if ($isResidual != 1 && $isResidual != 0);
	
    my $orthoVersion = $self->getArg('orthoVersion');

    my $dbReleaseId = $self->getDbRls();

    open ORTHO_FILE, "<$orthologFile";
    my $groupCount = 0;
    my $lineCount = 0;
    while (<ORTHO_FILE>) {
        chomp;
        $lineCount++;

        if ($self->_parseGroup($_, $isResidual, $orthoVersion, $dbReleaseId)) {
            $groupCount++;
            if (($groupCount % 1000) == 0) {
                $self->log("$groupCount ortholog groups loaded.");
            }
        } else {
            $self->log("line cannot be parsed:\n#$lineCount '$_'.");
        }
    }
    $self->log("total $lineCount lines processed, and $groupCount groups loaded.");
}

sub _parseGroup {
    my ($self, $line, $isResidual, $orthoVersion, $dbReleaseId) = @_;

    # example line: OG2_1009: osa|ENS1222992 pfa|PF11_0844
    if ($isResidual == 0) {
        if ($line = /^OG(\d+)_(\d+):\s.*/) {
            my $groupVersion = $1;
            my $groupNumber = $2;
            my $groupId;
            $groupId = 'OG' . $groupVersion . '_' . $groupNumber;
            # create a OrthlogGroup instance
            my $orthoGroup = GUS::Model::ApiDB::OrthologGroup->new({group_id => $groupId,
                                                                    is_residual => $isResidual,
                                                                    external_database_release_id => $dbReleaseId,
                                                                   });
            $orthoGroup->submit();
            $orthoGroup->undefPointerCache();
            return 1;
        }
        else {
            return 0;
        } 
    } 
    else {
        if ($line = /^OG(\d+):\s.*/) {
            my $groupNumber = $1;
            my $groupId;
            $groupId = 'OGR' . $orthoVersion . '_' . $groupNumber;
            # create a OrthlogGroup instance
            my $orthoGroup = GUS::Model::ApiDB::OrthologGroup->new({group_id => $groupId,
                                                                    is_residual => $isResidual,
                                                                    external_database_release_id => $dbReleaseId,
                                                                   });
            $orthoGroup->submit();
            $orthoGroup->undefPointerCache();
            return 1;
        }
        else {
            return 0;
        }
    }
}

sub getDbRls {
  my ($self) = @_;

  my $name = $self->getArg('extDbName');

  my $version = $self->getArg('extDbVersion');

  my $externalDatabase = GUS::Model::SRes::ExternalDatabase->new({"name" => $name});
  $externalDatabase->retrieveFromDB();

  if (! $externalDatabase->getExternalDatabaseId()) {
    $externalDatabase->submit();
  }
  my $external_db_id = $externalDatabase->getExternalDatabaseId();

  my $externalDatabaseRel = GUS::Model::SRes::ExternalDatabaseRelease->new ({'external_database_id'=>$external_db_id,'version'=>$version});

  $externalDatabaseRel->retrieveFromDB();

  if (! $externalDatabaseRel->getExternalDatabaseReleaseId()) {
    $externalDatabaseRel->submit();
  }

  my $external_db_rel_id = $externalDatabaseRel->getExternalDatabaseReleaseId();

  return $external_db_rel_id;
}

# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroup');
}

1;
