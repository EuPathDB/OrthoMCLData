package OrthoMCLData::Load::Plugin::InsertOrthomclOldIdsMap;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

my $argsDeclaration =
[
    fileArg({name           => 'oldIdsFastaFile',
            descr          => 'fasta file for old IDs.  defline has old ID with taxon prefix',
            reqd           => 1,
            mustExist      => 1,
	    format         => '>pfa|123445',
            constraintFunc => undef,
            isList         => 0, }),

    fileArg({name           => 'taxonMapFile',
            descr          => 'mapping from old taxon abbreviations to new',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'pfa pfal',
            constraintFunc => undef,
            isList         => 0, }),


];

my $purpose = <<PURPOSE;
Insert a mapping from old orthomcl sequence IDs to new ones.  
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert a mapping from old orthomcl sequence IDs to new ones.  
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
DoTS.ExternalAASequence
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
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

    my $taxonMapFile = $self->getArg('taxonMapFile');
    my $oldIdsFastaFile = $self->getArg('oldIdsFastaFile');

    my $taxonMap = getTaxonMap($taxonMapFile); #old taxon abbrev -> new abbrev
    my $oldIDsHash = getOldIds($oldIdsFastaFile); #taxon->oldId->1

    # process one taxon at a time
    foreach my $oldTaxon (keys(%$taxonMap)) {

	my $newIDsHash = getNewIds($taxonMap->{$oldTaxon}); # from db

	my $missingIDsHash = subtract($oldIDsHash->{$oldTaxon}, $newIDsHash);
	my $candidateIDsHash = subtract($newIDsHash, $oldIDsHash->{$oldTaxon});

	my $missingSeqHash = getMissingSeqHash($oldTaxon, $missingIDsHash, $oldIdsFastaFile);
	my $candidateSeqHash = getCandSeqHash($candidateIDsHash);

	foreach my $missingSeq (keys(%$missingSeqHash)) {
	    my $foundId = $candidateSeqHash->{$missingSeq};
	    if ($foundId) {
		insertMatch($missingSeqHash->{$missingSeq}, $foundId);
	    }
	}
    }
}

sub getTaxonMap {
    my ($taxonMapFile) = @_;
    open(F, $taxonMapFile);
    my $taxonMap;
    while (<F>) {
	chomp;
	my ($oldTaxon, $newTaxon) = split(/\s/);
	$taxonMap->{$oldTaxon} = $newTaxon;
    }
    return $taxonMap;
}

sub getOldIds {
    my ($oldIdsFastaFile) = @_;
    open(F, $oldIdsFastaFile);
    my $oldSeqMap;
    while (<F>) {
	chomp;
	# >pfa|PF11_0233
	if (/\>(\w+)\|(\S+)/) {
	    $oldSeqMap->{$1}->{$2} = 1;
	} 
    }
}

sub getNewIds {
    my ($taxonAbbrev) = @_;
    my $sql = "
select source_id
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.three_letter_abbrev = '$taxonAbbrev';
and ot.taxon_id = s.taxon_id
";

    my $newIds;
    my $stmt = $self->prepareAndExecute($sql);
    while (my ($sourceId) = $stmt->fetchrow_array()) {
      $newIds->{$sourceId} = 1;
    }
    return $newIds;
}

sub subtract {
    my ($idHash1, $idHash2) = @_;

    my $answer;
    
    # $idHash1 - $idHash2
    foreach my $id2 (keys (%$idHash2)) {
	if (!$idHash1->{$id2}) {
	    $answer->{$id1} = 1;
	}
    }
    return $answer;
}

sub getMissingSeqHash {
    my ($oldTaxon, $missingIDsHash, $oldIdsFastaFile) = @_;
    my $currentSeq;
    my $currentTaxon;
    my $currentId;
    my $missingSeqHash;
    open(F, $oldIdsFastaFile);
    while (<F>) {
	chomp;
	if (/\>(\w+)\|(\S+)/) {	    
	    if ($currentSeq) {
		if ($currentTaxon eq $oldTaxon && $missingIdsHash->{$currentId}) {
		    $missingSeqHash->{$currentSeq} = $currentId;
		}
		$currentSeq = "";
	    }
	    $currentTaxon = $1;
	    $currentId = $2;
	    
	} else {
	    $currentSeq .= "$_\n";
	}
    }
    if ($currentSeq) {
	if ($currentTaxon eq $oldTaxon && $missingIdsHash->{$currentId}) {
	    $missingSeqHash->{$currentSeq} = $currentId;
	}
    }
    return $missingSeqHash;
}

sub getCandSeqHash {
    my ($candidateIDsHash) = @_;
    my $sql = "select sequence
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.three_letter_abbrev = '$taxonAbbrev';
and ot.taxon_id = s.taxon_id
and s.source_id = ?";
    my $stmt = $self->prepare($sql);

    my $candSeqHash;
    foreach my $candId (keys(%$candidateIDsHash)) {
	$stmt->execute($candId);
	my ($seq) = $stmt->fetchrow_array();
	$candSeqHash->{$seq} = $candId;
    }
}

sub insertMatch {
    my ($oldId, $newId) = @_;
} 


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence',
          'ApiDB.OrthologGroup',
	 );
}

1;
