package OrthoMCLData::Load::Plugin::InsertOrthomclOldIdsMap;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

my $argsDeclaration =
[
    fileArg({name           => 'oldIdsFastaFile',
            descr          => 'fasta file for old IDs.  defline has old ID with taxon prefix.  gzipped file is allowed',
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

    my $taxonMap = $self->getTaxonMap($taxonMapFile); #old taxon abbrev -> new abbrev
    my $oldIDsHash = $self->getOldIds($oldIdsFastaFile); #taxon->oldId->1

    my $totalMappedCount;
    # process one taxon at a time
    my @sortedOldTaxons = sort(keys(%$taxonMap));
    foreach my $oldTaxon (@sortedOldTaxons) {
        $self->log("processing old taxon: '$oldTaxon'");

        $self->log("   number of old IDs: " . scalar(keys(%{$oldIDsHash->{$oldTaxon}})));
        my $newTaxon = $taxonMap->{$oldTaxon};
	my $newIDsHash = $self->getNewIds($newTaxon); # from db

	my $missingIDsHash = $self->subtract("old IDs from new IDs", $oldIDsHash->{$oldTaxon}, $newIDsHash);

	my $missingSeqHash = $self->getMissingSeqHash($oldTaxon, $missingIDsHash, $oldIdsFastaFile);

	my $candidateIDsHash = $self->subtract("new IDs from old IDs", $newIDsHash, $oldIDsHash->{$oldTaxon});
	my $candidateSeqHash = $self->getCandSeqHash($newTaxon, $candidateIDsHash);

	my $mappedCount;
	foreach my $missingSeq (keys(%$missingSeqHash)) {
	    my $foundId = $candidateSeqHash->{$missingSeq};
	    if ($foundId) {
		$self->insertMatch($missingSeqHash->{$missingSeq}, $foundId);
		$mappedCount++;
		$totalMappedCount++;
	    }
	}
	$self->log("   mapped $mappedCount");

    }
    return "mapped a total of $totalMappedCount proteins";
}

sub getTaxonMap {
    my ($self, $taxonMapFile) = @_;
    $self->log("Reading taxon map file") or die $!;
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
    my ($self, $oldIdsFastaFile) = @_;
    $self->log("Getting old IDs from $oldIdsFastaFile");
    if ($oldIdsFastaFile =~ /\.gz$/) {
      open(F, "zcat $oldIdsFastaFile|") or die $!;
    } else {
      open(F, $oldIdsFastaFile) or die $!;
    }
    my $oldIdsMap;
    my $count;
    while (<F>) {
	chomp;
	# >pfa|PF11_0233
	if (/\>(\w+)\|(\S+)/) {
	    $oldIdsMap->{$1} = {} unless $oldIdsMap->{$1};
	    print STDERR "duplicate ID $1 $2\n" if $oldIdsMap->{$1}->{$2};
	    $oldIdsMap->{$1}->{$2} = 1;
	    $count++;
	}
    }
    $self->log("Total number of old Taxa: " . scalar(keys(%$oldIdsMap)));
    $self->log("Total number of old IDs: $count");
    close(F);
    return $oldIdsMap;
}

sub getNewIds {
    my ($self, $taxonAbbrev) = @_;
    $self->log("   getting new IDs from db for taxon '$taxonAbbrev' ");
    my $sql = "
select source_id
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.three_letter_abbrev = '$taxonAbbrev'
and ot.taxon_id = s.taxon_id
";

    my $newIds;
    my $count;
    my $stmt = $self->prepareAndExecute($sql);
    while (my ($sourceId) = $stmt->fetchrow_array()) {
      $newIds->{$sourceId} = 1;
      $count++;
    }
    $self->log("   number of new IDs: $count");
    $self->error("Did not find any new IDs for taxon $taxonAbbrev") unless $count;
    return $newIds;
}

sub subtract {
    my ($self, $msg, $idHash1, $idHash2) = @_;

    my $answer;
    my @idArray1 = keys(%$idHash1);
    my @idArray2 = keys(%$idHash2);

    # $idHash1 - $idHash2
    foreach my $id1 (@idArray1) {
	$answer->{$id1} = 1 unless $idHash2->{$id1};
    }
    $self->log("   subtracting $msg = " . scalar(keys(%$answer)));
    return $answer;
}

sub getMissingSeqHash {
    my ($self, $oldTaxon, $missingIdsHash, $oldIdsFastaFile) = @_;
    my $currentSeq;
    my $currentTaxon;
    my $currentId;
    my $missingSeqHash;
    $self->log("   getting missing seqs hash");
    if ($oldIdsFastaFile =~ /\.gz$/) {
      open(F, "zcat $oldIdsFastaFile|") or die $!;
    } else {
      open(F, $oldIdsFastaFile) or die $!;
    }
    while (<F>) {
	chomp;
	if (/\>(\w+)\|(\S+)/) {
	    if ($currentSeq) {
		if ($currentTaxon eq $oldTaxon && $missingIdsHash->{$currentId}) {
		  if ($missingSeqHash->{$currentSeq}) {
		    print STDERR "duplicate seq $currentTaxon $currentId\n";
		  } else {
		    $missingSeqHash->{$currentSeq} = $currentId;
		  }
		}
		$currentSeq = "";
	    }
	    $currentTaxon = $1;
	    $currentId = $2;
	} else {
	    $currentSeq .= "$_";
	}
    }
    if ($currentSeq) {
	if ($currentTaxon eq $oldTaxon && $missingIdsHash->{$currentId}) {
	    $missingSeqHash->{$currentSeq} = $currentId;
	}
    }
    $self->log("   found " . keys(%$missingSeqHash) . " missing seqs");
    return $missingSeqHash;
}

sub getCandSeqHash {
    my ($self, $taxonAbbrev, $candidateIDsHash) = @_;
    $self->log("   getting candidate seqs hash");
    my $sql = "select sequence
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.three_letter_abbrev = '$taxonAbbrev'
and ot.taxon_id = s.taxon_id
and s.source_id = ?";
    my $stmt = $self->getQueryHandle()->prepare($sql);

    my $candSeqHash;
    foreach my $candId (keys(%$candidateIDsHash)) {
	$stmt->execute($candId);
	my ($seq) = $stmt->fetchrow_array();
	$candSeqHash->{$seq} = $candId;
    }
	$self->log("   found " . keys(%$candSeqHash) . " candidate seqs");
    return $candSeqHash;
}

sub insertMatch {
    my ($self, $oldId, $newId) = @_;

}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence',
          'ApiDB.OrthologGroup',
	 );
}

1;
