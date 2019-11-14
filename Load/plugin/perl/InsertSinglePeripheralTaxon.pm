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

    $self->log("The species '$taxonName' with abbrev '$abbrev' has been loaded into apidb.OrthomclTaxon.");
    
}


sub abbrevUnique {
    my ($abbrev) = @_;
    $abbrev=lc($abbrev);

    my $sql = "SELECT LOWER(three_letter_abbrev) FROM apidb.orthomcltaxon";
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

sub getTaxonId {
    my ($ncbiTaxonId) = @_;

    my $sql = "
SELECT t.taxon_id, tn.name
FROM sres.taxon t, sres.taxonname tn
WHERE t.ncbi_tax_id = $ncbiTaxonId
AND t.taxon_id = tn.taxon_id
AND tn.name_class = 'scientific name'
";
    my $stmt = $self->prepareAndExecute($sql);
    my ($taxonId, $taxonName) = $stmt->fetchrow_array();
    die "Failed to obtain taxonId or taxonName for ncbiTaxonId $ncbiTaxonId" if (! $taxonId || ! $taxonName);
    my ($testIfMore1, $testIfMore2) = $stmt->fetchrow_array();
    die "There is more than one entry for ncbiTaxonId $ncbiTaxonId" if ($testIfMore1 || $testIfMore2);
    return ($taxonId, $taxonName);
}

sub getSpeciesOrder {
    my $sql = "SELECT MAX(species_order) FROM apidb.OrthomclTaxon";
    my $stmt = $self->prepareAndExecute($sql);
    my ($maxOrder) = $stmt->fetchrow_array();
    die "Failed to obtain maximum species_order from apidb.OrthomclTaxon" if (! $maxOrder);
    return $maxOrder+1;
}

sub getCladeInfo {
    my ($orthomclClade) = @_;
    $orthomclClade = lc($orthomclClade);

    my $sql = "
SELECT orthomcl_taxon_id, depth_first_index
FROM apidb.OrthomclTaxon
WHERE LOWER(three_letter_abbrev) = '$orthomclClade'
AND taxon_id IS NULL
";
    my $stmt = $self->prepareAndExecute($sql);
    my ($parentId, $depthFirstIndex) = $stmt->fetchrow_array();
    die "Failed to obtain parentId or depthFirstIndex for orthomclClade $orthomclClade" if (! $parentId || ! $depthFirstIndex);
    my ($testIfMore1, $testIfMore2) = $stmt->fetchrow_array();
    die "There is more than one entry for orthomclClade $orthomclClade" if ($testIfMore1 || $testIfMore2);
    return ($parentId, $depthFirstIndex);
}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthomclTaxon'
	 );
}

1;
