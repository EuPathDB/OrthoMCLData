package OrthoMCLData::Load::Plugin::InsertOrthomclOldGroupsMap;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;
use GUS::Model::SRes::ExternalDatabase;
use GUS::Model::SRes::ExternalDatabaseRelease;
use GUS::Model::SRes::DbRef;
use GUS::Model::DoTS::AASequenceDbRef;

my $argsDeclaration =
[
    fileArg({name          => 'oldGroupsFile',
            descr          => 'groups file for a previous release of orthomcl',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG1_1222: pfa|PF11_0344, pvi|233245',
            constraintFunc => undef,
            isList         => 0, }),
    fileArg({name           => 'taxonMapFile',
            descr          => 'mapping from old taxon abbreviations to new',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'pfa pfal',
            constraintFunc => undef,
            isList         => 0, }),
   stringArg({name => 'dbVersion',
              descr => 'Version of old OrthoMCL whose groups will be mapped',
              constraintFunc=> undef,
              reqd  => 1,
              isList => 0
             }),
];

my $purpose = <<PURPOSE;
Insert a mapping from current orthomcl sequence IDs to old groups.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
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

  $self->initialize({ requiredDbVersion => 3.6,
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}



# ======================================================================

sub run {
    my ($self) = @_;

    my $version = $self->getArg('dbVersion');

    my $oldAbbrev2Taxon = $self->getOldAbbrev2TaxonHsh;

    my $oldSeqId2OldGroup = $self->getOldId2Group($oldAbbrev2Taxon);

    my $dbRlsId = $self->getExternalDatabaseRelease;

    # get map of current seq ID to old seq id
    my $sql = "
select s.taxon_id,s.aa_sequence_id, r.primary_identifier
from DoTS.AASequence s, DoTS.AASequenceDbRef sr, 
     SRes.DBref r, sres.externaldatabase db, sres.externaldatabaserelease dbr
where s.aa_sequence_id = sr.aa_sequence_id
  and r.db_ref_id = sr.db_ref_id and r.external_database_release_id = dbr.external_database_release_id
  and dbr.version = '$version' and dbr.external_database_id = db.external_database_id
  and db.name = 'OrthoMCL Old Seqs'";

    my $stmt = $self->prepareAndExecute($sql);
    my $count;
    while (my ($taxonId, $aaSeqId, $oldId) = $stmt->fetchrow_array()) {
      my $oldGroup = $oldSeqId2OldGroup->{"$taxonId|$oldId"};
      if ($oldGroup) {
	$self->insertMatch($aaSeqId, $oldGroup, $dbRlsId);
	$count++;
      }
    }
    return "Inserted $count";
}


sub getOldAbbrev2TaxonHsh {
  my ($self) = @_;

  my $newAbbrev2Taxon = $self->getNewAbbrev2TaxonHsh;

  my $abbrevMap = $self->getArg('taxonMapFile');

  my %oldAbbrev2Taxon;

  open(F, $abbrevMap);

  while (<F>) {
    chomp;
    my ($oldAbbrev, $newAbbrev) = split(/\s/);
    $oldAbbrev2Taxon{$oldAbbrev} = $newAbbrev2Taxon->{$newAbbrev};
  }
  my $num = scalar (keys %oldAbbrev2Taxon);

  $self->log ("$num old abbreviations mapped to taxons\n");
  return \%oldAbbrev2Taxon;
}

sub getNewAbbrev2TaxonHsh {
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


# return hash of old seq Id --> old group id
sub getOldId2Group {
    my ($self, $oldAbbrev2Taxon) = @_;

    my $oldGroupsFile = $self->getArg('oldGroupsFile');

    if ($oldGroupsFile =~ /\.gz$/) {
      open(F, "zcat $oldGroupsFile|") or die $!;
    } else {
      open(F, $oldGroupsFile) or die $!;
    }
    my %hash;
    while (<F>) {
	chomp;
	my @a = split(/\s/);
	my $g = shift @a;
	$g =~ s/\://;
	foreach my $id (@a) {
	  my ($abbrev,$sourceId) = split(/\|/,$id);
	  $hash{"$oldAbbrev2Taxon->{$abbrev}|$sourceId"} = $g;
	}
    }

    my $num = scalar (keys %hash);

    $self->log ("$num old source to old groups map\n");

    return \%hash;
}

sub insertMatch {
    my ($self, $aaSeqId, $oldGroup, $dbRlsId) = @_;

    my $lowercasePrimaryId = lc($oldGroup);

    my $dbRef = GUS::Model::SRes::DbRef -> new ({'lowercase_primary_identifier'=>$lowercasePrimaryId, 'external_database_release_id'=>$dbRlsId});
    $dbRef->retrieveFromDB();

    if (! $dbRef->getPrimaryIdentifier() || ($dbRef->getPrimaryIdentifier() && $dbRef->getPrimaryIdentifier() ne $oldGroup)) {
      $dbRef->setPrimaryIdentifier($oldGroup);
    }

    my $dbRefAASeq = GUS::Model::DoTS::AASequenceDbRef->new ({'aa_sequence_id'=>$aaSeqId});

    $dbRef->addChild($dbRefAASeq);

    my $rows += $dbRef->submit();

    $self->undefPointerCache();

    return $rows;

}

sub getExternalDatabaseRelease{

  my ($self) = @_;
  my $name = 'OrthoMCL Old Groups';

  my $externalDatabase = GUS::Model::SRes::ExternalDatabase->new({"name" => $name});
  $externalDatabase->retrieveFromDB();

  if (! $externalDatabase->getExternalDatabaseId()) {
    $externalDatabase->submit();
  }
  my $external_db_id = $externalDatabase->getExternalDatabaseId();

  my $version = $self->getArg('dbVersion');

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

  return ('DoTs.AASequenceDbRef'
       #  'SRes.DbRef',
	 );
}

1;
