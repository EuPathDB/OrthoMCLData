package OrthoMCLData::Load::Plugin::InsertOrthomclGroupTaxonMatrix;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::GroupTaxonMatrix;
use OrthoMCLData::Load::MatrixColumnManager;
use ApiCommonData::Load::Util;
use Data::Dumper;


my $argsDeclaration =
[

];

my $purpose = <<PURPOSE;
Insert a matrix of groups v. taxa. Each group gets a row.  Each species gets two columns, one with count of proteins in that group, and the other with 1 if there are any proteins, and 0 if none.  Each clade gets similar columns. The matrix is inserted into apidb.GroupTaxonMatrix. This table has 500 generic columns to hold the species and clades (column1, column2, ...).  the plugin uses ColumnManager to correctly map the species and clades into the generic columns.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert a matrix of groups v. taxa.
PURPOSE_BRIEF

my $notes = <<NOTES;

NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.GroupTaxonMatrix,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon, ApiDB.OrthologGroup

TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
Use the Undo plugin.
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
                      cvsRevision       => '$Revision: 19527 $',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}

# ======================================================================


# note: in this code, "taxa" or "taxon" refers to both species and clades.
sub run {
    my ($self) = @_;

    my $dbh = $self->getDbHandle();

    my $columnManager = OrthoMCLData::Load::MatrixColumnManager->new($dbh);

    $self->log("getting species and clades");
    ($self->{speciesClades}, $self->{taxaCount}) = $self->getSpeciesClades($dbh);

    $self->log("inserting rows");
    my $count = $self->insertRows($dbh, $columnManager);
    return "inserted $count rows"
}

sub insertRows {
    my ($self, $dbh, $columnManager) = @_;
    my $sql = "
SELECT g.ortholog_group_id, t.three_letter_abbrev, gs.aa_sequence_id
FROM apidb.orthologgroup g, apidb.orthomcltaxon t,
     apidb.orthologgroupaasequence gs, dots.aasequence s
WHERE gs.ortholog_group_id = g.ortholog_group_id
  AND s.aa_sequence_id = gs.aa_sequence_id
  AND t.taxon_id = s.taxon_id
ORDER BY g.ortholog_group_id
";

    my $prevGroupId;
    my $firstRow = 1;
    my $groupProteinsPerSpecies;
    my $count;
    my $stmt = $dbh->prepareAndExecute($sql);
    while (my ($groupId,$species) = $stmt->fetchrow_array()) {
      if ($groupId != $prevGroupId && !$firstRow) {
	$self->insertGroupIntoMatrix($prevGroupId, $groupProteinsPerSpecies,
				     $columnManager);
	$count++;
	$prevGroupId = $groupId;
	$groupProteinsPerSpecies = {};
      }
      $groupProteinsPerSpecies->{$species} += 1;
    }
    $self->insertGroupIntoMatrix($prevGroupId, $groupProteinsPerSpecies,
				 $columnManager);
    return $count+1;
}

# get a map of species to clades (transitive), plus a total count of species
# and clades
sub getSpeciesClades {
  my ($self, $dbh) = @_;

  my $taxaCount = 0;

  my $sql= "
SELECT three_letter_abbrev, depth_first_index, sibling_depth_first_index
FROM apidb.orthomcltaxon
WHERE is_species = 0
";
  my $stmt = $dbh->prepareAndExecute($sql);

  my $clades;
  while (my ($tla, $index, $sibIndex) = $stmt->fetchrow_array()) {
    $taxaCount++;
    $clades->{$tla} = [$index, $sibIndex];
  }

  my $sql= "
SELECT three_letter_abbrev, depth_first_index, sibling_depth_first_index
FROM apidb.orthomcltaxon
WHERE is_species = 0
";
  my $stmt = $dbh->prepareAndExecute($sql);

  my $species;
  while (my ($tla, $index) = $stmt->fetchrow_array()) {
    $taxaCount++;
    foreach my $clade (keys(%$clades)) {
      push(@{$species->{$tla}}, $clade)
	if ($index >= $clades->{$clade}->[0] 
	    && $index < $clades->{$clade}->[1]);
    }
  }
  return ($species,$taxaCount);
}

sub insertGroupIntoMatrix {
  my($self, $groupId, $groupProteinsPerSpecies, $columnManager) = @_;

  # initialized so all counts are 0 (not null)
  my $dbRow = $self->getInitializedRow($groupId);

  # populate clades with accumulated count of proteins
  # and while we're at it, populate species columns in db row
  my $cladesT;
  my $cladesP;
  foreach my $species (keys(%$groupProteinsPerSpecies)) {
    foreach my $cladeWithThisSpecies (@{$self->{speciesClades}->{$species}}) {
      $cladesP->{$cladeWithThisSpecies} += $groupProteinsPerSpecies->{$species};
      $cladesT->{$cladeWithThisSpecies} += 1;
    }
    my $speciesPCol = $columnManager->getColumnName($species, 'P');
    my $speciesTCol = $columnManager->getColumnName($species, 'T');
    eval "$dbRow->setColumn$speciesPCol($species)";
    eval "$dbRow->setColumn$speciesTCol(1)";
  }

  # now populate clade columns with accumulated counts
  foreach my $cladeWithThisSpecies (keys(%$cladesP)) {
    my $cladePCol = $columnManager->getColumnName($cladeWithThisSpecies, 'P');
    my $cladeTCol = $columnManager->getColumnName($cladeWithThisSpecies, 'T');
    eval "$dbRow->setColumn$cladePCol($cladesP->{$cladeWithThisSpecies})";
    eval "$dbRow->setColumn$cladeTCol($cladesT->{$cladeWithThisSpecies})";
  }
}

sub getInitializedRow {
  my ($self, $groupId) = @_;
  my $row = GUS::Model::ApiDB::GroupTaxonMatrix->new();
  $row->setOrthologGroupId($groupId);
  for (my $i=0; $i<$self->{taxaCount}*2; $i++) {
    my $colNum = $i + 1;
    eval "$row->setColumn$colNum(0)";
  }
  return $row;
}

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.GroupTaxonMatrix',
	 );
}



1;
