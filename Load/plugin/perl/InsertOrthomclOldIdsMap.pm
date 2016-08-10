package OrthoMCLData::Load::Plugin::InsertOrthomclOldIdsMap;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;
use GUS::Model::SRes::DbRef;
use GUS::Model::DoTS::AASequenceDbRef;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;

my $argsDeclaration =
[
    fileArg({name           => 'oldIdsFastaFile',
            descr          => 'fasta file for old IDs.  defline has old ID with taxon prefix.  gzipped file is allowed',
            reqd           => 1,
            mustExist      => 1,
	    format         => '>pfa|123445',
            constraintFunc => undef,
            isList         => 0, }),
    fileArg({name           => 'abbrevMapFile',
            descr          => 'mapping from old taxon abbreviation to new',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'pfa pfal',
            constraintFunc => undef,
            isList         => 0, }),
    stringArg({name           => 'oldReleaseNum',
            descr          => 'version of old OrthoMCL for externaldatbaserelease row',
            reqd           => 1,
            constraintFunc => undef,
            isList         => 0, })
];

my $purpose = <<PURPOSE;
Insert a mapping from old orthomcl sequence IDs to new ones.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
gaInsert a mapping from old orthomcl sequence IDs to new ones.  
PURPOSE_BRIEF

my $notes = <<NOTES;
Scan the provided old FASTA file to get mapping from old taxon to old IDs.  For each taxon, query db to get new IDs.  Compare these lists to find old IDs not in the new release.  And find new IDs not in old list (these are candidates for mapping to old IDs). For each candidate ID, make a hash from sequence to candidate IDs. Scan old FASTA file again and build hash from missing old ID to sequence.  For each old ID insert a mapping.  If it matches a new ID insert that.  Else if its sequence matches a new sequence, insert all new IDs that match.
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

  $self->initialize({ requiredDbVersion => 4,
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}

# ======================================================================

sub run {
  my ($self) = @_;

  my $taxonMapFile = $self->getArg('abbrevMapFile');
  my $oldIDsFastaFile = $self->getArg('oldIdsFastaFile');

  my $taxonMap = $self->getTaxonMap($taxonMapFile); #old taxon abbrev -> new abbrev
  my $oldIDsHash = $self->getOldIDs($oldIDsFastaFile); #taxon->oldID->1

  my $totalMappedCount;

  my $abbrevTaxonHsh = $self->getAbbrevTaxonHash;

  my $dbRlsId = $self->getExternalDatabaseRelease;

  # process one taxon at a time
  my @sortedOldTaxons = sort(keys(%$taxonMap));
  foreach my $oldTaxon (@sortedOldTaxons) {
    $self->log("processing old taxon: '$oldTaxon'");

    $self->log("   number of old IDs: " . scalar(keys(%{$oldIDsHash->{$oldTaxon}})));
    my $oldIDs = $oldIDsHash->{$oldTaxon};
    my $newTaxon = $taxonMap->{$oldTaxon};
    $self->error("Could not find new abbrev for '$oldTaxon'") unless $newTaxon;
    my $newIDsHash = $self->getNewIDs($newTaxon); # from db

    my $missingIDsHash = $self->subtract("old IDs minus new IDs", $oldIDs, $newIDsHash);

    # hash from old ID to its old sequence
    my $missingSeqHash = $self->getMissingSeqHash($oldTaxon, $missingIDsHash, $oldIDsFastaFile);

    # new IDs that are candidates to be mapped to an old ID
    my $candidateIDsHash = $self->subtract("new IDs minus old IDs", $newIDsHash, $oldIDs);

    # map from sequences in new release to matching candidate IDs
    my $candidateSeqHash = $self->getCandSeqHash($newTaxon, $candidateIDsHash);

    my $idMappedCount = 0;
    my $seqMappedCount = 0;
    my $mappedToCount = 0;
    foreach my $oldID (keys(%$oldIDs)) {
      if ($newIDsHash->{$oldID}) {
	$self->insertMatch($oldTaxon,$oldID, $oldID, $newTaxon, $abbrevTaxonHsh,$dbRlsId);
	$idMappedCount++;
      } else {
	my $foundIDs = $candidateSeqHash->{$missingSeqHash->{$oldID}};
	if ($foundIDs) {
	  $seqMappedCount++;
	  foreach my $foundID (@$foundIDs) {
	    $mappedToCount++;
	    $self->insertMatch($oldTaxon, $oldID, $foundID, $newTaxon, $abbrevTaxonHsh,$dbRlsId);
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
    open(F, $taxonMapFile) || die "Can't open abbrevMapFile '$taxonMapFile'\n";
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
    $self->error("Did not find any new sequence IDs for taxon '$taxonAbbrev'") unless $count;
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

# a mapping from sequences in the current release to IDs 
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

sub getAbbrevTaxonHash {
  my ($self) = @_;

  my $sql = "select three_letter_abbrev, taxon_id from apidb.OrthomclTaxon";

  my $stmt = $self->prepareAndExecute($sql);

  my %abbrevTaxonHsh;

  while (my ($abbrev, $taxonId) = $stmt->fetchrow_array()) {
    $abbrevTaxonHsh{$abbrev} = $taxonId;
  }

  $stmt->finish();

  return \%abbrevTaxonHsh;

}


sub insertMatch {
    my ($self, $oldAbbrev, $oldId, $newId, $newTaxon, $abbrevTaxonHsh, $dbRlsId) = @_;

    my $lowercasePrimaryId = lc($oldId);

    my $dbRef = GUS::Model::SRes::DbRef -> new ({'lowercase_primary_identifier'=>$lowercasePrimaryId, 'external_database_release_id'=>$dbRlsId});
    $dbRef->retrieveFromDB();

    if (! $dbRef->getPrimaryIdentifier() || ($dbRef->getPrimaryIdentifier() && $dbRef->getPrimaryIdentifier() ne $oldId)) {
      $dbRef->setPrimaryIdentifier($oldId);
    }

    if (! $dbRef->getSecondaryIdentifier() || ($dbRef->getSecondaryIdentifier() && $dbRef->getSecondaryIdentifier() ne "$oldAbbrev|$oldId")) {
      $dbRef->setSecondaryIdentifier("$oldAbbrev|$oldId");
    }

    my $taxonId = $abbrevTaxonHsh->{$newTaxon};

    my $sql = "select aa_sequence_id
from dots.ExternalAaSequence 
where taxon_id = $taxonId
and source_id = '$newId'";

    my $stmt = $self->getQueryHandle()->prepareAndExecute($sql);

    my ($aaSeqId) = $stmt->fetchrow_array();

    $stmt->finish();

    my $dbRefAASeq = GUS::Model::DoTS::AASequenceDbRef->new ({'aa_sequence_id'=>$aaSeqId});

    $dbRef->addChild($dbRefAASeq);

    my $rows += $dbRef->submit();

    $self->undefPointerCache();

    return $rows;

}

sub getExternalDatabaseRelease{

  my ($self) = @_;
  my $name = 'OrthoMCL Old Seqs';

  my $externalDatabase = GUS::Model::SRes::ExternalDatabase->new({"name" => $name});
  $externalDatabase->retrieveFromDB();

  if (! $externalDatabase->getExternalDatabaseId()) {
    $externalDatabase->submit();
  }
  my $external_db_id = $externalDatabase->getExternalDatabaseId();

  my $version = $self->getArg('oldReleaseNum');

  my $externalDatabaseRel = GUS::Model::SRes::ExternalDatabaseRelease->new ({'external_database_id'=>$external_db_id,'version'=>$version});

  $externalDatabaseRel->retrieveFromDB();

  if (! $externalDatabaseRel->getExternalDatabaseReleaseId()) {
    $externalDatabaseRel->submit();
  }
  my $extDbRlsId = $externalDatabaseRel->getExternalDatabaseReleaseId();
  return $extDbRlsId;

}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('DoTS.AASequenceDbRef',
	  'SRes.DbRef',
          
	 );
}

1;
