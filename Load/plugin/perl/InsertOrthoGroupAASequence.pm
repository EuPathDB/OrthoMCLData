package OrthoMCLData::Load::Plugin::InsertOrthoGroupAASequence;
use lib "$ENV{GUS_HOME}/lib/perl";
@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;
use GUS::Supported::Util;
use File::Temp qw/ tempfile /;
use POSIX qw/strftime/;
use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::ApiDB::OrthologGroupAASequence;
use GUS::Model::DoTS::OrthoAASequence;

my $argsDeclaration =
[
    fileArg({name           => 'orthoFile',
            descr          => 'Ortholog Data (ortho.mcl). OrthologGroupName(gene and taxon count) followed by a colon then the ids for the members of the group',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'OG2_1009: osa|ENS1222992 pfa|PF11_0844...',
            constraintFunc => undef,
            isList         => 0, }),

 stringArg({ descr => 'isResidual (0 or 1)',
	     name  => 'isResidual',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

stringArg({ descr => 'orthoVersion (7)',
	     name  => 'orthoVersion',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),

];

my $purpose = <<PURPOSE;
Insert an ApiDB::OrthologGroupAASequence from an orthomcl groups file.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Load an orthoMCL group sequence pair.
PURPOSE_BRIEF

my $notes = <<NOTES;
Need a script to create the mapping file.
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroupAASequence
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthologGroup,
DoTS.OrthoAASequence
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
                      cvsRevision       => '$Revision$',
                      name              => ref($self),
                      argsDeclaration   => $argsDeclaration,
                      documentation     => $documentation});

  return $self;
}

# ======================================================================

sub run {
    my ($self) = @_;

    my $orthologFile = $self->getArg('orthoFile');

    my $isResidual = $self->getArg('isResidual');
    die "The isResidual variable must be 1 or 0. It is currently set to '$isResidual'" if ($isResidual != 1 && $isResidual != 0);
	
    my %sequenceIdHash;
    my $sql = "SELECT aa_sequence_id, secondary_identifier FROM dots.orthoaasequence";
    my $dbh = $self->getQueryHandle();
    my $aaSequenceQuery = $dbh->prepare($sql);
    $aaSequenceQuery->execute();

    while (my ($aaSeqId , $seqId)= $aaSequenceQuery->fetchrow_array()) {
        $sequenceIdHash{$seqId} = $aaSeqId;
    }

    my $formattedFile = $self->formatInput($orthologFile, %sequenceIdHash);

    my ($ctrlFh, $ctrlFile) = tempfile(SUFFIX => '.dat');

    $self->loadGroupSequence($filteredFile, $ctrlFile);

}

# ---------------------- Subroutines ----------------------

sub formatInput {
    my ($self, $inputFile, $sequenceIds) = @_;

    my $outputFile = "$inputFile\_formatted.txt";

    open(IN, $inputFile) or die "Cannot open input file $inputFile for reading. Please check and try again\n$!\n\n";
    open(OUT, "> $outputFile") or die "Cannot open output file $outputFile for writing. Please check and try again\n$!\n\n";

    while (<IN>) {
        my $line = $_;
       
        my @groupAndSeqs =  split(/:\s/,$line);
        my $groupId = $groupAndSeqs[0];
        my $seqs = $groupAndSeqs[1];
        my @groupSeqs = split(/\s/,$seqs);

        my $numOfSeqs = @groupSeqs;
 
        if ($numOfSeqs == 0) {
            die "No Sequences assigned to group $groupId";
        }

        foreach my $seq (@groupSeqs) {
 
            print OUT "$groupId,$sequenceIdHash{$seq}\n";

        }    
        
    }
    close(IN);
    close(OUT);

    return $outputFile;
}

sub loadGroupSequence {
    my ($self, $inputFile, $ctrlFile) = @_;

    my $ctrlFile = "$ctrlFile.ctrl";
    my $logFile = "$ctrlFile.log";

    $self->writeConfigFile($ctrlFile, $inputFile);

    my $login = $self->getConfig->getDatabaseLogin();
    my $password = $self->getConfig->getDatabasePassword();
    my $dbiDsn = $self->getConfig->getDbiDsn();
    my ($dbi, $type, $db) = split(':', $dbiDsn);

    if($self->getArg('commit')) {
        my $exitstatus = system("sqlldr $login/$password\@$db control=$ctrlFile log=$logFile rows=2000 errors=0");
        if ($exitstatus != 0){
            die "ERROR: sqlldr returned exit status $exitstatus";
        }

        open(LOG, $logFile) or die "Cannot open log file $logFile: $!";
        while (<LOG>) {
            $self->log($_);
        }
        close LOG;
        unlink $logFile;
    }
    unlink $ctrlFile;
}

sub writeConfigFile {                                                                                                                                                                                      
    my ($self, $configFile, $inputFile) = @_;

    my $modDate = uc(strftime("%d-%b-%Y", localtime));
    my $database = $self->getDb();
    my $projectId = $database->getDefaultProjectId();
    my $userId = $database->getDefaultUserId();
    my $groupId = $database->getDefaultGroupId();
    my $algInvocationId = $database->getDefaultAlgoInvoId();
    my $userRead = $database->getDefaultUserRead();
    my $userWrite = $database->getDefaultUserWrite();
    my $groupRead = $database->getDefaultGroupRead();
    my $groupWrite = $database->getDefaultGroupWrite();
    my $otherRead = $database->getDefaultOtherRead();
    my $otherWrite = $database->getDefaultOtherWrite();

    open(CONFIG, "> $configFile") or die "Cannot open file $configFile For writing:$!";

  print CONFIG "LOAD DATA
CHARACTERSET UTF8
LENGTH SEMANTICS CHAR
INFILE '$inputFile'
APPEND
INTO TABLE ApiDB.OrthoGroupAASequence
REENABLE DISABLED_CONSTRAINTS
FIELDS TERMINATED BY ','
TRAILING NULLCOLS
(
ortholog_group_aa_sequence_id SEQUENCE(MAX,1),
group_id,
aa_sequence_id,
modification_date constant \"$modDate\",
user_read constant $userRead,
user_write constant $userWrite,
group_read constant $groupRead,
group_write constant $groupWrite,
other_read constant $otherRead,
other_write constant $otherWrite,
row_user_id constant $userId,
row_group_id constant $groupId,
row_project_id constant $projectId,
row_alg_invocation_id constant $algInvocationId
)\n";
  close CONFIG;
}

sub getConfig {
    my ($self) = @_;

    if(!$self->{config}) {
        my $gusConfigFile = $self->getArg('gusConfigFile');
        $self->{config} = GUS::Supported::GusConfig->new($gusConfigFile);
    }
    $self->{config}
}

# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence');
}

1;
