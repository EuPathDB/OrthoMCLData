package OrthoMCLData::Load::Plugin::InsertOrthomclTaxon;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclTaxon;

use ApiCommonData::Load::Util;
use Data::Dumper;


my $argsDeclaration =
[
    fileArg({name           => 'cladeFile',
            descr          => 'a file containing the clade tree',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'see Notes',
            constraintFunc => undef,
            isList         => 0, }),

    fileArg({name           => 'speciesFile',
            descr          => 'a file containing the species',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'see Notes',
            constraintFunc => undef,
            isList         => 0, }),

];

my $purpose = <<PURPOSE;
Insert the Orthomcl-DB specific taxonomy.  The clade input file has the tree flattened depth first.  we use the line number for the depth_first_index.  sibling_depth_first_index is the depth_first_index of the next clade at the same level in the hierarchy.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert the Orthomcl-DB specific taxonomy used by the "Phyletic Pattern Expression" (PPE) query.  
PURPOSE_BRIEF

my $notes = <<NOTES;
Both input files are both constructed manually as part of the Orthomcl-DB genome acquistion phase.

The speciesFile is a columnar file with these columns:
  - three_letter_abbrev
  - ncbi_tax_id
  - clade_three_letter_abbrev  # an index into the cladeFile

The cladesFile is a depth first serialization of the clade tree.  Each clade hasa three letter abbreviation, a display name, and a depth indicated by pipe characters

The head of a sample cladesFile looks like this: 
ALL All
|  ARC Archea
|  BAC Bacteria
|  |  PRO Protobacteria


NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclTaxon,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.Taxon,

TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
Use the Undo plugin first.
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
                      cvsRevision       => '$Revision: 9 $',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}

# ======================================================================

sub run {
    my ($self) = @_;

    my $cladeFile = $self->getArgs()->{cladeFile};
    my $speciesFile = $self->getArgs()->{speciesFile};

    # make taxon tree with clades only
    my ($taxonTree, $cladeHash) = $self->parseCladeFile($cladeFile);

    # add species to it
    $self->parseSpeciesFile($cladeHash, $speciesFile);
 
    # off you go to the database, and behave yourself
    $taxonTree->submit();
}

sub parseCladeFile {
    my ($self, $cladeFile) = @_;

    open(FILE, $cladeFile) || $self->userError("can't open clade file '$cladeFile'");

    my $depth_first_index = 0;
    my $lastCladePerLevel = [];
    my $cladeHash = {};
    my $rootClade;
    while(<FILE>) {
	chomp;
	$depth_first_index++;
	my $level;

	my $clade = GUS::Model::ApiDB::OrthomclTaxon->
	  new({depth_first_index => $depth_first_index,
	       sibling_depth_first_index => 99999});
 
	# handle a clade, which looks like the following:
	# |  |  PRO Protobacteria
	if (/^([\|\s\s]*)([A-Z]{3}) ([A-Z]\w+.*)/) {
	    $rootClade = $clade unless $rootClade;
	    $level = length($1)/3; #count of pipe chars
	    $clade->setThreeLetterAbbrev($2);
	    $clade->setName($3);
	    $clade->setIsSpecies(0);
	    $clade->setTaxonId(undef);
	    $cladeHash->{$clade->getThreeLetterAbbrev()} = $clade;
	} else {
	    $self->userError("invalid line in clade file: '$_'");
	}
	foreach my $lastClade (@{$lastCladePerLevel}) {
	    $lastClade->setSiblingDepthFirstIndex($depth_first_index);
	}
	$lastCladePerLevel->[$level] = $clade;
	$clade->setParent($lastCladePerLevel->[$level-1]) if $level;
    }

    return ($rootClade, $cladeHash);
}

sub parseSpeciesFile {
    my ($self, $cladeHash, $speciesFile) = @_;

    open(FILE, $speciesFile) || $self->userError("can't open species file '$speciesFile'");

    my $dbh = $self->getQueryHandle();
    my $sql = "SELECT taxon_id FROM sres.taxon
             WHERE  ncbi_tax_id = ?";

    my $stmt = $dbh->prepare($sql);
 
    while(<FILE>) {
	chomp;

	my $species = GUS::Model::ApiDB::OrthomclTaxon->new();

	# pfa 123345 API
	if (/([a-z]{3})\t([A-Z]{3})\t(\d+)/) {
	    $species->setThreeLetterAbbrev($1);
	    my $clade = $cladeHash->{$2};
	    $species->setTaxonId($self->getTaxonId($stmt, $3));
	    $clade || die "can't find clade with code '$3' for species '$1'\n";
	    $species->setParent($clade);
	    $species->setIsSpecies(1);
	    $species->setName('fake name');
	    $species->setDepthFirstIndex($clade->getDepthFirstIndex());
	}  else {
	    $self->userError("invalid line in species file: '$_'");
	}
    }
}

sub getTaxonId {
    my ($self, $stmt, $ncbiTaxId) = @_;


  my @ids = $self->sqlAsArray( Handle => $stmt, Bind => [$ncbiTaxId] );

  if(scalar @ids != 1) {
    $self->error("Should return one value for ncbi_tax_id '$ncbiTaxId'");
  }
  return $ids[0];

}

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthomclTaxon',
	 );
}



1;
