package OrthoMCLData::Load::Plugin::InsertOrthomclGroupTaxonMatrix;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;
use Data::Dumper;

my $argsDeclaration =
[

];

my $purpose = <<PURPOSE;
Calculate number of proteins and number of taxa per orthogroup per species, including for each clade. This creates a new table for this: ApiDB.OrthologGroupTaxon
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Creates a new table: ApiDB.OrthologGroupTaxon, which houses number of proteins and taxa per orthogroup
PURPOSE_BRIEF

my $notes = <<NOTES;

NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroupTaxon,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon, ApiDB.OrthologGroup, ApiDB.OrthologGroupAaSequence, Dots.ExternalAaSequence,
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

  $self->initialize({ requiredDbVersion => 4,
                      cvsRevision       => '$Revision$',
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

    $self->log("creating table apidb.orthologgrouptaxon with number of proteins per species per orthologgroup");
    my $numSpeciesRows = $self->createTable($dbh);
    $self->log("created table with $numSpeciesRows rows");

    $self->log("adding rows for number of proteins per clade per orthogroup");
    my $speciesToClades = $self->getSpeciesToClades($dbh);
    my $numCladeRows = $self->addCladeRows($dbh,$speciesToClades);
    $self->log("added $numCladeRows rows");

}


sub createTable {
    my ($self, $dbh) = @_;

    my $sql = <<EOF;
CREATE TABLE apidb.orthologgrouptaxon (
    three_letter_abbrev,
    number_of_proteins,
    number_of_taxa,
    ortholog_group_id,
    CONSTRAINT orthoGroupTax_pk1 PRIMARY KEY (three_letter_abbrev,number_of_proteins,number_of_taxa,ortholog_group_id)
    )
ORGANIZATION index
NOLOGGING
AS    
SELECT substr(eas.secondary_identifier,1,4),
       count(ogas.aa_sequence_id),
       1 as number_of_taxa, og.ortholog_group_id
FROM apidb.orthologgroup og, apidb.orthologgroupaasequence ogas, dots.ExternalAaSequence eas        
WHERE ogas.ortholog_group_id = og.ortholog_group_id
  AND og.core_peripheral_residual in ('P','R')
  AND eas.aa_sequence_id = ogas.aa_sequence_id                                                    
GROUP BY substr(eas.secondary_identifier,1,4), og.ortholog_group_id 
EOF

    $dbh->prepareAndExecute($sql);
    $dbh->commit();


    $sql = "SELECT count(*) from apidb.orthologgrouptaxon";
    my $stmt = $dbh->prepareAndExecute($sql);
    my @row = $stmt->fetchrow_array();
    return $row[0];
}

sub getSpeciesToClades {
    my ($self,$dbh) = @_;

    my %tree;
    my %clades;
    my %species;

    my $sql = <<EOF;
SELECT orthomcl_taxon_id, parent_id, three_letter_abbrev, core_peripheral
FROM apidb.orthomcltaxon
EOF

    my $stmt = $dbh->prepareAndExecute($sql);
    while ( my ($id, $parent, $name, $type) = $stmt->fetchrow_array() ) {
	$tree{$id}=$parent if ($parent);
	if ($type eq 'Z') {
	    $clades{$id}=$name;
	} else {
	    $species{$id}=$name;
	}
    }

    my $speciesToClades;
    foreach my $speciesId (keys %species) {
	my $parents=[];
	getParents($parents,$speciesId,\%tree);
	my @parentNames = map { $clades{$_} } @{$parents};
	$speciesToClades->{$species{$speciesId}} = [];
	push $speciesToClades->{$species{$speciesId}}, @parentNames;
    }

    return $speciesToClades;
}

sub getParents {
    my ($parents, $speciesId, $tree) = @_;    
    if (exists $tree->{$speciesId}) {
	push @{$parents}, $tree->{$speciesId};
	getParents($parents, $tree->{$speciesId}, $tree);
    }
}

sub addCladeRows {
    my ($self, $dbh, $speciesToClades) = @_;

    my $clades;
    my $sql = <<EOF;
SELECT three_letter_abbrev,number_of_proteins,number_of_taxa,ortholog_group_id
FROM apidb.orthologgrouptaxon
EOF

    my $stmt = $dbh->prepareAndExecute($sql);
    while (my ($name, $numProteins, $numTaxa, $orthoId) = $stmt->fetchrow_array()) {
	foreach my $clade (@{$speciesToClades->{$name}}) {
	    $clades->{$clade}->{$orthoId}->{numTaxa} += $numTaxa;
	    $clades->{$clade}->{$orthoId}->{numProteins} += $numProteins;
	}
    }

    my $numCladeRows = 0;
    $sql = <<EOF;
INSERT INTO apidb.orthologgrouptaxon (three_letter_abbrev,number_of_proteins,number_of_taxa,ortholog_group_id)
VALUES (?,?,?,?)
EOF
    $stmt = $dbh->prepare($sql);
    foreach my $clade (keys %{$clades}) {
	foreach my $orthoId (keys %{$clades->{$clade}}) {
	    my $numProteins = $clades->{$clade}->{$orthoId}->{numProteins};
	    my $numTaxa = $clades->{$clade}->{$orthoId}->{numTaxa};
	    $stmt->execute($clade,$numProteins,$numTaxa,$orthoId);
	    $dbh->commit();
	    $numCladeRows++;
	}
    }

    return $numCladeRows;
}

# ----------------------------------------------------------------


sub undoTables {
  my ($self) = @_;

  return ( );
}


sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;

    my $sql = "DROP TABLE ApiDB.OrthologGroupTaxon PURGE";

    my $sql = <<SQL;
          BEGIN
	      EXECUTE IMMEDIATE 'DROP TABLE ApiDB.OrthologGroupTaxon PURGE';
          EXCEPTION
	      WHEN OTHERS THEN
	         IF SQLCODE != -942 THEN
		     RAISE;
                 END IF;
           END;
SQL

    print STDERR "executing sql: $sql\n";
    my $queryHandle = $dbh->prepare($sql) or die $dbh->errstr;
    $queryHandle->execute() or die $dbh->errstr;

}


1;
