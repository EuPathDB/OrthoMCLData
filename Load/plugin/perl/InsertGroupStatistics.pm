package OrthoMCLData::Load::Plugin::InsertGroupStatistics;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthoGroupCorePeripheralStats;
use GUS::Model::ApiDB::OrthoGroupCoreStats;
use GUS::Model::ApiDB::OrthoGroupResidualStats;

# use ApiCommonData::Load::Util;


my $argsDeclaration =
[
    fileArg({name           => 'groupStatsFile',
            descr          => 'Ortholog Groups Statistics. OrthologGroupName followed by Stats of the group.',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG0000000 3 22.25 30.2666666666667 6 6 1 1.18233333333333e-05',
            constraintFunc => undef,
            isList         => 0, }),

  stringArg({ descr => 'Either C P or R. Indicates if the stats are from the core, core and peripheral, or residual processes',
 	     name  => 'corePeripheralResidual',
	     isList    => 0,
             mustExist      => 1,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

# stringArg({ descr => 'Name of the External Database',
#	     name  => 'extDbName',
#	     isList    => 0,
#	     reqd  => 1,
#	     constraintFunc => undef,
#	   }),


# stringArg({ descr => 'Version of the External Database Release',
#	     name  => 'extDbVersion',
#	     isList    => 0,
#	     reqd  => 1,
#	     constraintFunc => undef,
#	   }),

];

my $purpose = <<PURPOSE;
Insert core, core + peripheral and residual group statistics into ApiDB::OrthoGroup<core,coreperipheral,residual>Stats
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert core, core + peripheral and residual group statistics into ApiDB::OrthoGroup<core,coreperipheral,residual>Stats
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthoGroupCoreStats
ApiDB.OrthoGroupCorePeripheralStats
ApiDB.OrthoGroupResidualStats
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.ExternalDatabase,
Sres.ExternalDatabaseRelease,
Sres.ExternalAASequence
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, since the same ortholog group from the same OrthoMCL analysis version will only be loaded once.
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
                      cvsRevision       => '$Revision: 68598 $',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}



# ======================================================================

sub run {
    my ($self) = @_;

    my $groupStatsFile = $self->getArg('groupStatsFile');

    my $corePeripheralResidual = $self->getArg('corePeripheralResidual');
    $corePeripheralResidual = uc($corePeripheralResidual);
    die "The corePeripheralResidual variable must be C or P or R. It is currently set to '$corePeripheralResidual'" if ($corePeripheralResidual !~ /^[CPR]$/);
	
    #my $dbReleaseId = $self->getDbRls();

    open STATS_FILE, "<$groupStatsFile";
    my $lineCount = 0;
    while (<STATS_FILE>) {
        chomp;
        $lineCount++;
        # example line: OG0000000 3 22.25 30.2666666666667 6 6 1 1.18233333333333e-05 (groupId\tnumberOfProteins\tavgMatchPercent\tavgPIdent\tsimilarityPairCount\tmaxPossiblePairsWithSimilarity\tpercentPairsWithSimilarity\tavgEValue)
        if (/^(OG\S+)\t(\d+)\t(.+)\t(.+)\t(\d+)\t(\d+)\t(.+)\t(.+)/) {
            my $groupId = $1;
            my $numberOfProteins = $2;
            my $avgMatchPercent = $3;
            my $avgPIdent = $4;
            my $similarityPairCount = $5;
            my $maxPossiblePairsWithSimilarity = $6;
            my $percentPairsWithSimilarity = $7;
            my $avgEValue = $8;
            my $evaluemant;
            my $evalueexp;
            if ($avgEValue == 1) {
                $evaluemant = 1;
                $evalueexp = 0;
            }
            else {
                my @eValuePieces = split(/e-/, $avgEValue);
	        $evaluemant = @eValuePieces[0];
	        $evalueexp = @eValuePieces[1];
            }
            if ($corePeripheralResidual eq 'C') {
                my $orthoGroupCoreStats = GUS::Model::ApiDB::OrthoGroupCoreStats->
                    new({
                        ortho_group_id => $groupId,
                        number_of_members => $numberOfProteins,
                        avg_percent_match => $avgMatchPercent,
                        avg_percent_identity => $avgPIdent,
                        number_of_match_pairs => $similarityPairCount,
                        max_possible_pairs => $maxPossiblePairsWithSimilarity,
                        percent_match_pairs => $percentPairsWithSimilarity,
                        avg_evalue_mant => $evaluemant,
                        avg_evalue_exp => $evalueexp     
                    });
                $orthoGroupCoreStats->submit();
                $orthoGroupCoreStats->undefPointerCache();
            }
            elsif ($corePeripheralResidual eq 'P') {
                if ($avgMatchPercent eq 'NA') {
                    $avgMatchPercent = 'NULL';
                    $avgPIdent = 'NULL';
                    $evaluemant = 'NULL';
                    $evalueexp = 'NULL';
                }  
                my $orthoGroupCorePeripheralStats = GUS::Model::ApiDB::OrthoGroupCorePeripheralStats->
                    new({
                        ortho_group_id => $groupId,
                        number_of_members => $numberOfProteins,
                        avg_percent_match => $avgMatchPercent,
                        avg_percent_identity => $avgPIdent,
                        number_of_match_pairs => $similarityPairCount,
                        max_possible_pairs => $maxPossiblePairsWithSimilarity,
                        percent_match_pairs => $percentPairsWithSimilarity,
                        avg_evalue_mant => $evaluemant,
                        avg_evalue_exp => $evalueexp     
                    });
                $orthoGroupCorePeripheralStats->submit();
                $orthoGroupCorePeripheralStats->undefPointerCache();
            }
            elsif ($corePeripheralResidual eq 'R') {
                my $orthoGroupResidualStats = GUS::Model::ApiDB::OrthoGroupResidualStats->
                    new({
                        ortho_group_id => $groupId,
                        number_of_members => $numberOfProteins,
                        avg_percent_match => $avgMatchPercent,
                        avg_percent_identity => $avgPIdent,
                        number_of_match_pairs => $similarityPairCount,
                        max_possible_pairs => $maxPossiblePairsWithSimilarity,
                        percent_match_pairs => $percentPairsWithSimilarity,
                        avg_evalue_mant => $evaluemant,
                        avg_evalue_exp => $evalueexp     
                    });
                $orthoGroupResidualStats->submit();
                $orthoGroupResidualStats->undefPointerCache();
            }
            else {
                die "The corePeripheralResidual variable must be C or P or R. It is currently set to '$corePeripheralResidual'";
            }
        } else {
            $self->log("gene cannot be parsed: '$_'.");
	}
    }	    
    $self->log("total $lineCount lines processed.");
}

#sub getDbRls {
#  my ($self) = @_;
#
#  my $name = $self->getArg('extDbName');
#
#  my $version = $self->getArg('extDbVersion');
#
#  my $externalDatabase = GUS::Model::SRes::ExternalDatabase->new({"name" => $name});
#  $externalDatabase->retrieveFromDB();
#
#  if (! $externalDatabase->getExternalDatabaseId()) {
#    $externalDatabase->submit();
#  }
#  my $external_db_id = $externalDatabase->getExternalDatabaseId();

#  my $externalDatabaseRel = GUS::Model::SRes::ExternalDatabaseRelease->new ({'external_database_id'=>$external_db_id,'version'=>$version});

#  $externalDatabaseRel->retrieveFromDB();

#  if (! $externalDatabaseRel->getExternalDatabaseReleaseId()) {
#    $externalDatabaseRel->submit();
#  }

#  my $external_db_rel_id = $externalDatabaseRel->getExternalDatabaseReleaseId();

#  return $external_db_rel_id;
#}




# use full form of input id "pfa|PF11_0344"
sub getAASequenceId {
  my ($self, $inputId) = @_;

  if (!$self->{idMap}) {
    my $sql = "
select aa_sequence_id, source_id, three_letter_abbrev
from apidb.OrthomclTaxon ot, dots.ExternalAaSequence s
where ot.taxon_id = s.taxon_id
";

    my $stmt = $self->prepareAndExecute($sql);
    while (my ($sequenceId, $sourceId, $taxonId) = $stmt->fetchrow_array()) {
      $self->{idMap}->{"$taxonId|$sourceId"} = $sequenceId;
    }
  }
  return $self->{idMap}->{$inputId};
}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthoGroupCoreStats',
          'ApiDB.OrthoGroupCorePeripheralStats',
          'ApiDB.OrthoGroupResidualStats'
         );
}

sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});

    my $cpr = "";

    my $sql ="
SELECT ap.string_value
FROM CORE.ALGORITHMPARAM ap, core.algorithmparamkey apk
WHERE ap.ALGORITHM_PARAM_KEY_ID = apk.ALGORITHM_PARAM_KEY_ID
      AND ap.ROW_ALG_INVOCATION_ID IN ($rowAlgInvocations)
      AND apk.ALGORITHM_PARAM_KEY = 'corePeripheralResidual'";

    my $sh = $dbh->prepareAndExecute($sql);
    while (my @row = $sh->fetchrow_array()) {
	die "The corePeripheralResidual value is not C, P or R" if ($row[0] !~ /^[CPR]$/);
	die "There are multiple corePeripheralResidual values for this step" if ($cpr ne "" && $cpr ne $row[0]);
	$cpr = $row[0];
    }
    $sh->finish();

    if ($cpr eq 'C') {
        $sql = "DELETE FROM apidb.OrthoGroupCoreStats";
    }
    elsif ($cpr eq 'P') {
        $sql = "DELETE FROM apidb.OrthoGroupCorePeripheralStats";
    }
    elsif ($cpr eq 'R') {
        $sql = "DELETE FROM apidb.OrthoGroupResidualStats";
    }
    $sh = $dbh->prepareAndExecute($sql);
    $sh->finish();

}

1;
