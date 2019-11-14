package OrthoMCLData::Load::Plugin::InsertSinglePeripheralTaxon;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclTaxon;

my $argsDeclaration =
[
 stringArg({name           => 'abbrev',
            descr          => '4-letter abbreviation for the epripheral species. this must be unique',
            reqd           => 1,
            constraintFunc => undef,
            isList         => 0, }),


 stringArg({ descr => 'four-letter orthomcl clade to which the peripheral species belongs',
	     name  => 'orthomclClade',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),


 stringArg({ descr => 'The NCBI taxon id of the peripheral species',
	     name  => 'ncbiTaxonId',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

];

my $purpose = <<PURPOSE;
Insert peripheral species info into ApiDB::OrthomclTaxon table. The taxon_id is obtained from the NCBI taxon id.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert peripheral species info into ApiDB::OrthomclTaxon table.
PURPOSE_BRIEF

my $notes = <<NOTES;
Need to enter info into the dataset file regarding the peripheral species, into OrthoMCL.xml file.
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclTaxon
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.Taxon,
Sres.TaxonName
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

    my $abbrev = $self->getArg('abbrev');
    my $orthomclClade = $self->getArg('orthomclClade');
    my $ncbiTaxonId = $self->getArg('ncbiTaxonId');

    die "species abbreviation $abbrev must have 4 letters" if (length($abbrev) != 4);
    die "species abbreviation $abbrev already exists in the database" if (! abbrevUnique($abbrev));
    
    my ($taxonId, $taxonName) = getTaxonId($ncbiTaxonId);
    my $speciesOrder = getSpeciesOrder();
    my ($parentId, $depthFirstIndex) = getCladeInfo($orthomclClade);

    #get all existing clades from apidb.OrthomclTaxon, where taxon_id is null, test that the given clade is there and get the orthomcl_taxon_id, which will be the parent_id
    #get taxon_id and species name from ncbiTaxonId using taxon tables
    #is_species=1
    # species_order = max(species_order)+1
    # depth_first_index = depth_first_index of taxon
    #create OrthomclTaxon object and add these things and submit

    my $species = GUS::Model::ApiDB::OrthomclTaxon->
	new({parent_id => $parentId,
	     taxon_id => $taxonId,
	     name => $taxonName,
	     three_letter_abbrev => $abbrev,
	     is_species => 1,
	     species_order => $speciesOrder,
	     depth_first_index => $depthFirstIndex
	    });
    $species->submit();
    $species->undefPointerCache();

    $self->log("The species '$taxonName' with abbrev '$abbrev' has been loaded.");
    
}


sub abbrevUnique {
    my ($abbrev) = @_;

    my $sql = "select three_letter_abbrev from apidb.orthomcltaxon";
    my $stmt = $self->prepareAndExecute($sql);
    my %abbrevs;
    while (my ($currentAbbrev) = $stmt->fetchrow_array()) {
	$abbrevs->{$currentAbbrev} = 1;
    }

    if ( exists $abbrevs->{$abbrev} ) {
	return 0;
    } else {
	return 1;
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

  return ('ApiDB.OrthomclTaxon'
	 );
}

1;
