package OrthoMCLData::Load::Plugin::InsertOrthologousGroups;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthologGroup;
use GUS::Model::ApiDB::OrthologGroupAaSequence;

use ApiCommonData::Load::Util;


my $argsDeclaration =
[
    fileArg({name           => 'orthoFile',
            descr          => 'Ortholog Data (ortho.mcl). OrthologGroupName(gene and taxon count) followed by a colon then the ids for the members of the group',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'ORTHOMCL9(446 genes,1 taxa): osa1088(osa) osa1089(osa) osa11015(osa)...',
            constraintFunc => undef,
            isList         => 0, }),


 stringArg({ descr => 'Name of the External Database',
	     name  => 'extDbName',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   }),


 stringArg({ descr => 'Version of the External Database Release',
	     name  => 'extDbVersion',
	     isList    => 0,
	     reqd  => 1,
	     constraintFunc => undef,
	   })
];

my $purpose = <<PURPOSE;
The purpose of this plugin is to insert complete rows representing orthologous groups.  Each time the plugin is run, a new ApiDB::OrthologGroup is inserted.  Each child of OrthologGroup represents a line from the orthologFile (ie an orthologGroup).  For each orthoId which can be mapped to a source_id a row in ApiDB::OrthologGroupAASequence is inserted.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Load an orthoMCL analysis result.
PURPOSE_BRIEF

my $notes = <<NOTES;
Need a script to create the mapping file.
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthologGroup,
ApiDB.OrthologGroupAASequence
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
Sres.TaxonName,
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

    my $orthologFile = $self->getArgs()->{orthoFile};
    my $dbReleaseId = $self->getExtDbRlsId($self->getArgs()->{extDbName}, 
 				           $self->getArgs()->{extDbVersion});

    $self->log("Loading ortholog group file");

    # parse group file
    $self->_parseGroupFile($orthologFile, $dbReleaseId);
}


# ----------------------------------------------------------------------

sub _parseGroupFile {
    my ($self, $orthologFile, $dbReleaseId) = @_;

    open ORTHO_FILE, "<$orthologFile";
    my $groupCount = 0;
    my $lineCount = 0;
    while (<ORTHO_FILE>) {
        chomp;
        $lineCount++;

        if (1 == $self->_parseGroup($_, $dbReleaseId)) {
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
    my ($self, $line, $dbReleaseId) = @_;
    
    # example line: ORTHOMCL79685(2 genes,2 taxa): 81703(spn) 32290(ban)
    if ($line = /^(\S+?)\((\d+) genes\,(\d+) taxa\)\:(.*)/) {
        my $groupName = $1;
        my $geneCount = $2;
        my $taxonCount = $3;
        my @genes = split(' ', $4);

        # print "group=$groupName, #genes=$geneCount, #taxon=$taxonCount\n";

        # create a OrthlogGroup instance
        my $orthoGroup = GUS::Model::ApiDB::OrthologGroup->
            new({name => $groupName,
                 number_of_members => $geneCount,
                 external_database_release_id => $dbReleaseId,
                });

        for (@genes) {
            if (/(\S+?)\((\S+?)\)/) {
		my $sequenceId = $1;

		# create a OrthologGroupAASequence instance
		my $orthoGroupSequence = GUS::Model::ApiDB::OrthologGroupAaSequence->
		    new({aa_sequence_id => $sequenceId,
		      })->setParent($orthoGroup);
	    } else {
                $self->log("gene cannot be parsed: '$_'.");
            }
        }
        $orthoGroup->submit();
        $orthoGroup->undefPointerCache();

        return 1;
    } else {
        return 0;
    }
}


# ----------------------------------------------------------------------

sub undoTables {
  my ($self) = @_;

  return ('ApiDB.OrthologGroupAASequence',
          'ApiDB.OrthologGroup',
	 );
}

1;
