package OrthoMCLData::Load::Plugin::RetireCoreOrganisms;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

my $argsDeclaration =
[
 stringArg({ descr => 'comma-delimited list of core abbreviations to be changed to xxxx-old',
	     name  => 'abbrevList',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'main OrthoMCL data directory',
	     name  => 'mainDataDir',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'skip the abbrev if it has already been changed, as opposed to die',
	     name  => 'skip',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'the cluster name like consign.pmacs.upenn.edu',
	     name  => 'cluster',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'the cluster username or login',
	     name  => 'clusterUser',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),
 stringArg({ descr => 'the cluster data directory',
	     name  => 'clusterDir',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

];

my $purpose = <<PURPOSE;
change all instances of core abbrev, like aaeg, to aaeg-old, in files and database tables.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
change all instances of core abbrev, like aaeg, to aaeg-old, in files and database tables.
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclTaxon,
DoTS.ExternalAaSequence,
SRes.DbRef,
SRes.ExternalDatabase,
SRes.ExternalDatabaseRelease,
ApiDB.InparalogCore,
ApiDB.CoOrthologCore,
ApiDB.OrthologCore
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon,
DoTS.ExternalAaSequence,
SRes.DbRef,
SRes.ExternalDatabase,
SRes.ExternalDatabaseRelease,
ApiDB.InparalogCore,
ApiDB.CoOrthologCore,
ApiDB.OrthologCore
TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
The plugin can been restarted, update should only affect rows that have not been updated.
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

    my $cluster = $self->getArg('cluster');
    my $clusterUser = $self->getArg('clusterUser');
    my $clusterDir = $self->getArg('clusterDir');
    my $mainDataDir = $self->getArg('mainDataDir');
    my $abbrevList = $self->getArg('abbrevList');
    my $skip = $self->getArg('skip');
    if (lc($skip) eq 'true' || lc($skip) eq 'yes') {
	$skip=1;
    } else {
	$skip=0;
    }

    my $abbrevsToChange = $self->getProperAbbrevs($abbrevList,$skip);
    my $num = keys %{$abbrevsToChange};
    if ($num == 0) {
	$self->log("There are no proper abbreviations. Not updating anything.\n");
    } else {
	my $text = join(",",keys %{$abbrevsToChange});
	$self->log("There are $num proper abbreviations. Changing these:\n  $text\n");
	$self->updateTables($abbrevsToChange);
	$self->updateFiles($abbrevsToChange,$mainDataDir,$cluster,$clusterUser,$clusterDir);
	$self->log("Finished updating tables and files.\n");
    }
}


sub getProperAbbrevs {
    my ($self,$abbrevList,$skip)=@_;

    $abbrevList =~ s/ //g;
    my %abbrevs = map { $_ => 1 } split(/,/,$abbrevList);

    my $currentCore = $self->getCurrentCore();

    my $properAbbrevs;

    foreach my $abbrev (keys %abbrevs) {
	my $oldAbbrev = $abbrev."-old";
	if (exists $currentCore->{$oldAbbrev}) {
	    $self->log("You are trying to change '$abbrev' to '$oldAbbrev', but $oldAbbrev already exists.\n");
	    die if (! $skip);
	} elsif (! exists $currentCore->{$abbrev}) {
	    $self->log("'$abbrev' does not exist as a Core organism (or at all).\n");
	    die if (! $skip);
	} else {
	    $properAbbrevs->{$abbrev} = $currentCore->{$abbrev};
	}
    }

    return $properAbbrevs;
}

sub getCurrentCore {
    my ($self) = @_;

    my $sql = <<"SQL";
SELECT o.three_letter_abbrev,o.name,edr.version
FROM apidb.orthomcltaxon o
      , sres.ExternalDatabase ed
      , sres.ExternalDatabaseRelease edr
WHERE o.three_letter_abbrev = NVL(SUBSTR(ed.name, 0, INSTR(ed.name, '_')-1), ed.name)
      AND ed.external_database_id = edr.external_database_id
      AND ed.name like '%Proteome%'
      AND o.core_peripheral = 'C'
SQL

    my $core;
    my $dbh = $self->getQueryHandle();
    my $sth = $dbh->prepareAndExecute($sql);
    while (my @row = $sth->fetchrow_array()) {
	my $name = $row[1]." (old build ".$row[2].")";
	$core->{$row[0]} = $name;
    }

    return $core;
}


sub updateTables {
   my ($self,$abbrevsToChange) = @_;
   $self->log("Updating database tables.\n");
   foreach my $abbrev (keys %{$abbrevsToChange}) {
       my $sqls = getSql();
       foreach my $sql (@{$sqls}) {
	   $sql =~ s/#abbrev#/$abbrev/g;
	   my $name = $abbrevsToChange->{$abbrev};
	   $sql =~ s/#name#/$name/g;
	   $self->log("$sql\n");	   
	   my $dbh = $self->getQueryHandle();
	   $dbh->{RaiseError} = 1;
	   $dbh->{AutoCommit} = 1;
	   my $sth = $dbh->prepareAndExecute($sql);
	   my $rowCount = $sth->rows;
	   $self->log("Updated $rowCount rows.\n");
       }
   }
}

sub updateFiles {
   my ($self,$abbrevsToChange,$mainDataDir,$cluster,$clusterUser,$clusterDir) = @_;
   $self->log("Updating database tables.\n");
   my $files = getFiles();
   foreach my $file (@{$files}) {
       my $fullPath = $mainDataDir.$file;
       my $gz = $fullPath.".gz";
       if (-e $gz) {system("gunzip $gz")};
       if (! -e $fullPath) {die "File '$fullPath' does not exist.\n"}
       $self->log("Updating file '$fullPath'.\n");
       foreach my $abbrev (keys %{$abbrevsToChange}) {
	   my $changed = $abbrev."-old|";
	   $abbrev .= "|";
	   my $cmd = "sed -i 's/$abbrev/$changed/g' $fullPath";
	   system($cmd);
       }
       if ($file eq 'coreGood.fasta') {
	   my $cmd = "scp $fullPath $clusterUser@$cluster:\"$clusterDir\"";
	   system($cmd);
	   $cmd = "ssh -2 $clusterUser@$cluster \"cd $clusterDir; formatdb -i coreGood.fasta -p T\"";
	   system($cmd);
       }
   }
}

sub getFiles {
    my @files = (
	"coreGood.fasta"
	,
	"coreGroups/orthomclGroups.txt"
	,
	"coreGroups/pairs/coorthologs.txt"
	,
	"coreGroups/pairs/inparalogs.txt"
	,
	"coreGroups/pairs/orthologs.txt"
	);

    return \@files;
}

sub getSql {

    my @sql = (
	"UPDATE apidb.OrthomclTaxon SET name = '#name#', three_letter_abbrev = '#abbrev#-old' WHERE three_letter_abbrev = '#abbrev#'"
	,
	"UPDATE dots.ExternalAaSequence SET secondary_identifier = '#abbrev#-old' || SUBSTR(secondary_identifier, INSTR(secondary_identifier,'|')) WHERE secondary_identifier LIKE '#abbrev#|%'"
	,
	"UPDATE sres.DbRef SET secondary_identifier = '#abbrev#-old' || SUBSTR(secondary_identifier, INSTR(secondary_identifier,'|')) WHERE secondary_identifier LIKE '#abbrev#|%'"
	,
	"UPDATE sres.ExternalDatabase SET name = '#abbrev#-old' || SUBSTR(name, INSTR(name,'_')) WHERE name LIKE '#abbrev#_%Proteome%'"
	,
	"UPDATE apidb.InParalogCore SET sequence_id_a = '#abbrev#-old' || SUBSTR(sequence_id_a, INSTR(sequence_id_a,'|')), sequence_id_b = '#abbrev#-old' || SUBSTR(sequence_id_b, INSTR(sequence_id_b,'|')), taxon_id = '#abbrev#-old' WHERE taxon_id = '#abbrev#'"
	,
	"UPDATE apidb.CoOrthologCore SET sequence_id_a = '#abbrev#-old' || SUBSTR(sequence_id_a, INSTR(sequence_id_a,'|')), taxon_id_a = '#abbrev#-old' WHERE taxon_id_a = '#abbrev#'"
	,
	"UPDATE apidb.CoOrthologCore SET sequence_id_b = '#abbrev#-old' || SUBSTR(sequence_id_b, INSTR(sequence_id_b,'|')), taxon_id_b = '#abbrev#-old' WHERE taxon_id_b = '#abbrev#'"
	,
	"UPDATE apidb.OrthologCore SET sequence_id_a = '#abbrev#-old' || SUBSTR(sequence_id_a, INSTR(sequence_id_a,'|')), taxon_id_a = '#abbrev#-old' WHERE taxon_id_a = '#abbrev#'"
	,
	"UPDATE apidb.OrthologCore SET sequence_id_b = '#abbrev#-old' || SUBSTR(sequence_id_b, INSTR(sequence_id_b,'|')), taxon_id_b = '#abbrev#-old' WHERE taxon_id_b = '#abbrev#'"
	);

    return \@sql;
}


# ----------------------------------------------------------------------

sub undoUpdateTables {
  my ($self) = @_;

  return (
	 );
}


sub undoTables {
  my ($self) = @_;

  return (
         );
}


sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});
}

1;
