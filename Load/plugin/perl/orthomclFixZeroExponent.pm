package OrthoMCLData::Load::Plugin::orthomclFixZeroExponent;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use GUS::Model::ApiDB::OrthologGroup;
use Data::Dumper;

my $argsDeclaration =
[
  stringArg({name          => 'simSeqTableSuffix',
            descr          => 'The Suffix of the SimilarSequences table that is going to be edited.',
            reqd           => 1,
            mustExist      => 1,
            constraintFunc => undef,
            isList         => 0, }),

];

my $purpose = <<PURPOSE;
update ApiDB::SimilarSequences(suffix) table to change 0 in exponent to lowest exponent. This is required for the clusterLayout step. Otherwise, these are treated like 1e0 = 1
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
update ApiDB::SimilarSequences(suffix) table to change 0 in exponent to lowest exponent.
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.,SimilarSequences(suffix)
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
SimilarSequences(suffix),
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, plugin will only update rows with null multiple_sequence_alignment.
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

    my $simSeqTableSuffix = $self->getArg('simSeqTableSuffix');
    $self->log("Getting the minimum exponent in table apidb.similarSequences$simSeqTableSuffix \n");
    my $minExponent = $self->getMinExponent($simSeqTableSuffix);
    $self->log("The minimum exponent is $minExponent \n");
    $self->log("Updating table: all zero exponents altered to the minimum of $minExponent \n");
    $self->setZeroToMin($simSeqTableSuffix,$minExponent);
    $self->log("Finished updating table. \n");
}


sub getMinExponent {
  my ($self,$simSeqTableSuffix) = @_;

  my $sql = <<"SQL";
     SELECT min(evalue_exp)
     FROM apidb.similarSequences$simSeqTableSuffix
SQL

  my $dbh = $self->getQueryHandle();
  my $sth = $dbh->prepareAndExecute($sql);
  my @row = $sth->fetchrow_array();
  $sth->finish();
  return $row[0];
}

sub setZeroToMin {
  my ($self, $simSeqTableSuffix, $minExponent) = @_;

  my $chunkSize = 100000;

  my $sql = <<SQL;
      UPDATE apidb.similarSequences$simSeqTableSuffix
      SET evalue_exp = $minExponent, evalue_mant = 1
      WHERE evalue_exp = 0 AND rownum <= $chunkSize
SQL

  my $dbh = $self->getQueryHandle();
  my $updateStmt = $dbh->prepare($sql);
  my $numRowsUpdated = 0;

  while (1) {
      my $rtnVal = $updateStmt->execute() or die $dbh->errstr;
      $numRowsUpdated += $rtnVal;
      $self->log("Updated $numRowsUpdated rows");
      $dbh->commit() || die "Committing updates failed: " . $dbh->errstr() . "\n";
      last if $rtnVal < $chunkSize;
  }

}



# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;
  return (
	 );
}

1;
