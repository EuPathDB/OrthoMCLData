package OrthoMCLData::Load::Plugin::InsertOrthoGroupAASequence;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

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
	
    my $orthoVersion = $self->getArg('orthoVersion');

    open ORTHO_FILE, "<$orthologFile";
    my $groupCount = 0;
    my $lineCount = 0;
    while (<ORTHO_FILE>) {
        chomp;
        $lineCount++;

        if ($self->_parseGroup($_, $isResidual, $orthoVersion)) {
            $groupCount++;
            if (($groupCount % 1000) == 0) {
                $self->log("$groupCount ortholog groups loaded.");
            }
        } else {
            $self->log("line cannot be parsed:\n#$lineCount '$_'.");
        }
    }
    $self->log("total $lineCount lines processed, and $groupCount groups loaded.");
}

sub _parseGroup {
    my ($self, $line, $isResidual, $orthoVersion) = @_;

    my $dbh = $self->getQueryHandle();

    # example line: OG2_1009: osa|ENS1222992 pfa|PF11_0844
    my $groupId;
    my @groupSeqs;
    if ($isResidual == 0) {
        if ($line = /^(OG\d+_\d+):\s(.*)/) {
            $groupId = $1;
            @groupSeqs = split(/\s/,$2);
        }
        else {
            die "Improper groupFile format";
	}
    }
    else {
        if ($line = /^(OGR\d+_\d+):\s(.*)/) {
            $groupId = $1;
            @groupSeqs = split(/\s/,$2);
        }
        else {
            die "Improper groupFile format";
	}
    }

    my $numOfSeqs = @groupSeqs;
 
    if ($numOfSeqs == 0) {
        die "No Sequences assigned to group";
    }

    foreach my $groupSeq (@groupSeqs) {
        my $sql = "SELECT aa_sequence_id FROM dots.orthoaasequence WHERE SECONDARY_IDENTIFIER = '$groupSeq'";
        my $aaSequenceQuery = $dbh->prepare($sql);
        $aaSequenceQuery->execute();
        my @data = $aaSequenceQuery->fetchrow_array();
        my $aaSequence = $data[0];
        # create a OrthlogGroupAASequence instance
        my $orthoGroupAASequence = GUS::Model::ApiDB::OrthologGroupAASequence->new({group_id => $groupId,
                                                                                    aa_sequence_id => $aaSequence
                                                                                   });
        $orthoGroupAASequence->submit();
        $orthoGroupAASequence->undefPointerCache();
    }
    return 1;
}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence');
}

1;
