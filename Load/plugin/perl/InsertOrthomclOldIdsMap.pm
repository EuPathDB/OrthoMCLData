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
  my $oldIDsFastaFile = $self->getArg('oldIdsFastaFile');

  my $taxonMap = $self->getTaxonMap($taxonMapFile); #old taxon abbrev -> new abbrev
  my $oldIDsHash = $self->getOldIDs($oldIDsFastaFile); #taxon->oldID->1

  my $totalMappedCount;
  # process one taxon at a time
  my @sortedOldTaxons = sort(keys(%$taxonMap));
  foreach my $oldTaxon (@sortedOldTaxons) {
    $self->log("processing old taxon: '$oldTaxon'");

    $self->log("   number of old IDs: " . scalar(keys(%{$oldIDsHash->{$oldTaxon}})));
    my $oldIDs = $oldIDsHash->{$oldTaxon};
    my $newTaxon = $taxonMap->{$oldTaxon};
    my $newIDsHash = $self->getNewIDs($newTaxon); # from db

    my $missingIDsHash = $self->subtract("old IDs from new IDs", $oldIDs, $newIDsHash);

    my $missingSeqHash = $self->getMissingSeqHash($oldTaxon, $missingIDsHash, $oldIDsFastaFile);

    my $candidateIDsHash = $self->subtract("new IDs from old IDs", $newIDsHash, $oldIDs);
    my $candidateSeqHash = $self->getCandSeqHash($newTaxon, $candidateIDsHash);

    my $idMappedCount = 0;
    my $seqMappedCount = 0;
    my $mappedToCount = 0;
    foreach my $oldID (keys(%$oldIDs)) {
      if ($newIDsHash->{$oldID}) {
	$self->insertMatch($oldID, $oldID);
	$idMappedCount++;
      } else {
	my $foundIDs = $candidateSeqHash->{$missingSeqHash->{$oldID}};
	if ($foundIDs) {
	  $seqMappedCount++;
	  foreach my $foundID (@$foundIDs) {
	    $mappedToCount++;
	    $self->insertMatch($oldID, $foundID);
	  }
	}
      }
      $totalMappedCount++;
    }
    $self->log("   mapped $idMappedCount by ID; $seqMappedCount by seq (mapped to $mappedToCount new IDs)");
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

sub getOldIDs {
    my ($self, $oldIDsFastaFile) = @_;
    $self->log("Getting old IDs from $oldIDsFastaFile");
    if ($oldIDsFastaFile =~ /\.gz$/) {
      open(F, "zcat $oldIDsFastaFile|") or die $!;
    } else {
      open(F, $oldIDsFastaFile) or die $!;
    }
    my $oldIDsMap;
    my $count;
    while (<F>) {
	chomp;
	# >pfa|PF11_0233
	if (/\>(\w+)\|(\S+)/) {
	    $oldIDsMap->{$1} = {} unless $oldIDsMap->{$1};
	    print STDERR "duplicate ID $1 $2\n" if $oldIDsMap->{$1}->{$2};
	    $oldIDsMap->{$1}->{$2} = 1;
	    $count++;
	}
    }
    $self->log("Total number of old Taxa: " . scalar(keys(%$oldIDsMap)));
    $self->log("Total number of old IDs: $count");
    close(F);
    return $oldIDsMap;
}

sub getNewIDs {
    my ($self, $taxonAbbrev) = @_;
    $self->log("   getting new IDs from db for taxon '$taxonAbbrev' ");
    my $sql = "
select source_id
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.three_letter_abbrev = '$taxonAbbrev'
and ot.taxon_id = s.taxon_id
";

    my $newIDs;
    my $count;
    my $stmt = $self->prepareAndExecute($sql);
    while (my ($sourceID) = $stmt->fetchrow_array()) {
      $newIDs->{$sourceID} = 1;
      $count++;
    }
    $self->log("   number of new IDs: $count");
    $self->error("Did not find any new IDs for taxon $taxonAbbrev") unless $count;
    return $newIDs;
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
    my ($self, $oldTaxon, $missingIDsHash, $oldIDsFastaFile) = @_;
    my $currentSeq;
    my $currentTaxon;
    my $currentID;
    my $missingSeqHash;
    my $duplicateSeqs;
    $self->log("   getting missing seqs hash");
    if ($oldIDsFastaFile =~ /\.gz$/) {
      open(F, "zcat $oldIDsFastaFile|") or die $!;
    } else {
      open(F, $oldIDsFastaFile) or die $!;
    }
    while (<F>) {
	chomp;
	if (/\>(\w+)\|(\S+)/) {
	    if ($currentSeq) {
		if ($currentTaxon eq $oldTaxon && $missingIDsHash->{$currentID}) {
		  $missingSeqHash->{$currentID} = $currentSeq;
		}
		$currentSeq = "";
	    }
	    $currentTaxon = $1;
	    $currentID = $2;
	} else {
	    $currentSeq .= "$_";
	}
    }
    if ($currentSeq) {
	if ($currentTaxon eq $oldTaxon && $missingIDsHash->{$currentID}) {
	    $missingSeqHash->{$currentID} = $currentSeq;
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
    my $duplicateSeqs;
    foreach my $candID (keys(%$candidateIDsHash)) {
	$stmt->execute($candID);
	my ($seq) = $stmt->fetchrow_array();
	$candSeqHash->{$seq} = [] unless $candSeqHash->{$seq};
	push(@{$candSeqHash->{$seq}}, $candID);
    }
    $self->log("   found " . keys(%$candSeqHash) . " candidate seqs");
    return $candSeqHash;
}

sub insertMatch {
    my ($self, $oldID, $newID) = @_;

}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence',
          'ApiDB.OrthologGroup',
	 );
}

1;
