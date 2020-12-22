package OrthoMCLData::Load::Plugin::InsertSinglePeripheralTaxonNameVersion;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclTaxon;

my $argsDeclaration =
[
 stringArg({name           => 'abbrev',
            descr          => '4-letter abbreviation for the peripheral species. this must be unique',
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

 stringArg({ descr => 'The version of the proteome',
	     name  => 'version',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

 stringArg({ descr => 'The full name of the organism to be shown on the site',
	     name  => 'organismName',
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
    my $version = $self->getArg('version');
    my $organismName = $self->getArg('organismName');

    die "species abbreviation '$abbrev' must have 4 letters" if (length($abbrev) != 4);
    die "species abbreviation '$abbrev' already exists in the database" if (! $self->abbrevUnique($abbrev));
    
    my ($taxonId, $taxonName) = $self->getTaxonId($ncbiTaxonId);
    my $speciesOrder = $self->getSpeciesOrder();
    my ($parentId, $depthFirstIndex) = $self->getCladeInfo($orthomclClade);

    my $species = GUS::Model::ApiDB::OrthomclTaxon->new();
    $species->set('parent_id', $parentId);
    $species->set('taxon_id', $taxonId);
    $species->set('name', $organismName);
    $species->set('three_letter_abbrev', $abbrev);
    $species->set('is_species', 1);
    $species->set('species_order', $speciesOrder);
    $species->set('depth_first_index', $depthFirstIndex);
    $species->set('core_peripheral', 'P');
    $species->submit();
    $species->undefPointerCache();

    $self->log("The species '$organismName' with abbrev '$abbrev' has been loaded into apidb.OrthomclTaxon.");
    
}


sub abbrevUnique {
    my ($self, $abbrev) = @_;

    my $sql = "SELECT three_letter_abbrev FROM apidb.orthomcltaxon";
    my $stmt = $self->prepareAndExecute($sql);
    my %abbrevs;
    while (my ($currentAbbrev) = $stmt->fetchrow_array()) {
	$abbrevs{$currentAbbrev} = 1;
    }

    if ( exists $abbrevs{$abbrev} ) {
	return 0;
    } else {
	return 1;
    }
}

sub getTaxonId {
    my ($self, $ncbiTaxonId) = @_;

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
    my ($self) = @_;
    my $sql = "SELECT MAX(species_order) FROM apidb.OrthomclTaxon";
    my $stmt = $self->prepareAndExecute($sql);
    my ($maxOrder) = $stmt->fetchrow_array();
    die "Failed to obtain maximum species_order from apidb.OrthomclTaxon" if (! $maxOrder);
    return $maxOrder+1;
}

sub getCladeInfo {
    my ($self, $orthomclClade) = @_;

    my $sql = "
SELECT orthomcl_taxon_id, depth_first_index
FROM apidb.OrthomclTaxon
WHERE three_letter_abbrev = '$orthomclClade'
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

  return (
	 );
}


sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});

    my %abbrev;
    my $sql = " 
SELECT ap.string_value
FROM CORE.ALGORITHMPARAM ap, core.algorithmparamkey apk
WHERE ap.ALGORITHM_PARAM_KEY_ID = apk.ALGORITHM_PARAM_KEY_ID
      AND ap.ROW_ALG_INVOCATION_ID IN ($rowAlgInvocations)
      AND apk.ALGORITHM_PARAM_KEY = 'abbrev'";
    my $sh = $dbh->prepareAndExecute($sql);
    while (my @row = $sh->fetchrow_array()) {
	$abbrev{$row[0]} = 1;
    }
    $sh->finish();

    my $oneAbbrev;
    my $n = keys %abbrev;
    if ($n > 1) {
	$self->log("Error. There should be one abbrev value for this step but there are $n: ");
	$self->log("'".$_."' ") foreach (keys %abbrev);
	$self->("\n");
	die;
    } elsif ($n == 0) {
        $self->log("Error. There is no abbrev value for this step.\n");
	die;
    } else {
	$oneAbbrev = $_ foreach (keys %abbrev);
    }

    $sql = "
DELETE FROM apidb.OrthologGroup
WHERE three_letter_abbrev = '$oneAbbrev'";

    $sh = $dbh->prepareAndExecute($sql);
    $sh->finish();
}

1;
