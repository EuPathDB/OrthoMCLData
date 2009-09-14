package OrthoMCLData::Load::Plugin::InsertOrthomclOldGroupsMap;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

my $argsDeclaration =
[
    fileArg({name          => 'oldGroupsFile',
            descr          => 'groups file for a previous release of orthomcl',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG1_1222: pfa|PF11_0344, pvi|233245',
            constraintFunc => undef,
            isList         => 0, }),

   stringArg({name => 'externalDatabaseSpec',
              descr => 'External database to write the seq IDs of a previous release of orthomcl ',
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

    my $oldGroupsFile = $self->getArg('oldGroupsFile');

    my $oldSeqId2OldGroup = $self->getOldId2Group($oldGroupsFile);

    # get map of current seq ID to old seq id
    my $sql = "
select s.source_id, r.primary_identifier
from DoTS.AASequence s, DoTS.AASequenceDbRef sr, 
     SRes.DBref r
where s.aa_sequence_id = sr.aa_sequence_id
  and r.db_ref_id = sr.db_ref_id 
";

    my $stmt = $self->prepareAndExecute($sql);
    my $count;
    while (my ($sourceId, $oldId) = $stmt->fetchrow_array()) {
	$self->insertMatch($sourceId, $oldSeqId2OldGroup->{$oldId});
	$count++;
    }
    return "Inserted $count";
}

# return hash of old seq Id --> old group id
sub getOldId2Group {
    my ($self, $oldGroupsFile) = @_;

    if ($oldGroupsFile =~ /\.gz$/) {
      open(F, "zcat $oldGroupsFile|") or die $!;
    } else {
      open(F, $oldGroupsFile) or die $!;
    }
    my $hash;
    while (<F>) {
	chomp;
	my @a = split(/\s/);
	my $g = shift @a;
	$g =~ s/\://;
	foreach my $id (@a) {
	    $hash->{$id} = $g;
	}
    }
    return $hash;
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
