package OrthoMCLData::Load::Plugin::InsertMappedPeripheralsIntoCoreGroups;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::ApiDB::OrthologGroupAaSequence;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;

# use ApiCommonData::Load::Util;


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
Insert peripheral proteins into an exisiting core  ApiDB::OrthologGroup as described in an orthomcl groups file.
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

  $self->initialize({ requiredDbVersion => 4,
                      cvsRevision       => '$Revision: 68598 $',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}



# ======================================================================

sub run {
    my ($self) = @_;

    my $orthologFile = $self->getArg('orthoFile');

    my $dbReleaseId = $self->getDbRls();

    open ORTHO_FILE, "<$orthologFile";
    my $groupCount = 0;
    my $lineCount = 0;
    my $notFound;
    while (<ORTHO_FILE>) {
        chomp;
        $lineCount++;

        if ($self->_parseGroup($_, $dbReleaseId)) {
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


sub _parseGroup {
    my ($self, $line, $dbReleaseId) = @_;

    # example line: OG6_1009: osat|ENS1222992 pfal|PF11_0844
    if ($line = /^(\S+)\: (.*)/) {
        my $groupName = $1;
        my @genes = split(/\s+/, $2);
	my $geneCount = scalar(@genes);

        # get peripheral OrthlogGroup Id
        my $orthoGroupId = $self->getOrthoGroupId($groupName,$dbReleaseId);
	die "Can't find an ortholog_group_id for $groupName\n" if !$orthoGroupId;

        for (@genes) {
            if (/(\w+)\|(\S+)/) {
		my $taxonAbbrev = $1;
		my $sourceId = $2;
		my $sequenceId = $self->getAASequenceId($taxonAbbrev,$sourceId);
		die "Can't find an aa_sequence_id for abbrev:$taxonAbbrev source_id:$sourceId\n" if !$sequenceId;
		my $orthoGroupSequence = GUS::Model::ApiDB::OrthologGroupAaSequence->
		    new({aa_sequence_id => $sequenceId,
			 ortholog_group_id => $orthoGroupId
		       });
		$orthoGroupSequence->submit();
		$orthoGroupSequence->undefPointerCache();
	    } else {
                $self->log("gene cannot be parsed: '$_'.");
            }
        }
        return 1;
    } else {
        return 0;
    }
}


sub getOrthoGroupId {
  my ($self, $inputGroupName, $dbReleaseId) = @_;

  if (!$self->{groupMap}) {
    my $sql = "
select name, ortholog_group_id
from apidb.orthologgroup
where core_peripheral_residual = 'P'
and external_database_release_id = $dbReleaseId
";
    my $stmt = $self->prepareAndExecute($sql);
    while (my ($groupName, $groupId) = $stmt->fetchrow_array()) {
      $self->{groupMap}->{$groupName} = $groupId;
    }
  }
  if ( exists $self->{groupMap}->{$inputGroupName} ) {
      return $self->{groupMap}->{$inputGroupName};
  } else {
      die "Can't find ortholog group id for $inputGroupName";
  }
}


# use full form of input id "pfal|PF11_0344"
sub getAASequenceId {
  my ($self, $taxon, $inputId) = @_;
  $inputId=$taxon."|".$inputId;
  if (!$self->{aaMap}) {
    my $sql = "
select aa_sequence_id, secondary_identifier
from dots.ExternalAaSequence
where secondary_identifier like '$taxon%'
";
    my $stmt = $self->prepareAndExecute($sql);
    while (my ($sequenceId, $seqName) = $stmt->fetchrow_array()) {
      $self->{aaMap}->{$seqName} = $sequenceId;
    }
  }
  if ( exists $self->{aaMap}->{$inputId} ) {
      return $self->{aaMap}->{$inputId};
  } else {
      die "Can't find AA sequence id for $inputId";
  }
}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence'
	 );
}

1;
