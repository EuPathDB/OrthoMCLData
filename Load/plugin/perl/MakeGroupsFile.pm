package OrthoMCLData::Load::Plugin::MakeGroupsFile;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use ApiCommonData::Load::Util;
use Data::Dumper;


my $argsDeclaration =
[
    fileArg({name           => 'outputFile',
            descr          => 'a file for outputting the group info',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'see Notes',
            constraintFunc => undef,
            isList         => 0, })
];

my $purpose = <<PURPOSE;
Make the OrthoMCL groups file for the download site.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Make the OrthoMCL groups file for the download site.
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
This does not change the database.
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
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}

# ======================================================================

sub run {
    my ($self) = @_;

    my $outputFile = $self->getArgs()->{outputFile};
    open(OUTFILE, ">$outputFile") or die("Cannot open file '$outputFile' for writing\n");
    
    my $dbh = $self->getQueryHandle();
    my $sql_groups = "SELECT name, ortholog_group_id FROM ApiDB.OrthologGroup";
    my $query_groups = $dbh->prepare($sql_groups);
    
    my $sql_sequences_by_group = "SELECT ot.three_letter_abbrev || '|' || eas.source_id
FROM apidb.OrthologGroup og,
     apidb.OrthologGroupAaSequence ogs,
     dots.ExternalAaSequence eas,
     apidb.OrthomclTaxon ot
WHERE og.ortholog_group_id = ogs.ortholog_group_id
  AND ogs.aa_sequence_id = eas.aa_sequence_id
  AND eas.taxon_id = ot.taxon_id
  AND og.ortholog_group_id = ?";
    my $query_sequences_by_group = $dbh->prepare($sql_sequences_by_group);

    $query_groups->execute();

    while (my @data = $query_groups->fetchrow_array()) {
	print OUTFILE "$data[0]: ";
	my @groupseqs;
	$query_sequences_by_group->execute($data[1]);
	while (my @seqdata = $query_sequences_by_group->fetchrow_array()) {
	    push(@groupseqs, $data[0]);
	}
	print OUTFILE "join($groupseqs, ' ')\n";
    }
    
}

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthomclTaxon',
	 );
}

1;
