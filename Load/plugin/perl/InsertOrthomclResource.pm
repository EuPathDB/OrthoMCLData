package OrthoMCLData::Load::Plugin::InsertOrthomclResource;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclResource;

use ApiCommonData::Load::Util;
use Data::Dumper;

my $argsDeclaration =
[
    fileArg({name           => 'resourceFile',
            descr          => 'a tab-delimited file containing the resources',
            reqd           => 1,
            mustExist      => 1,
	    format         => 'see Notes',
            constraintFunc => undef,
            isList         => 0, }),
];


my $purpose = <<PURPOSE;
Insert the Orthomcl-DB specific resource information.  The resource input file is tab-delimited, with each line representing a single resource, providing the name and url of the data source for each specie.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert the Orthomcl-DB specific resource information used by the OrthoMCL Data Source page.  
PURPOSE_BRIEF

my $notes = <<NOTES;
The speciesFile is a columnar file with these columns:
  - three_letter_abbrev
  - description
  - strain
  - source_name
  - source_url



NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclResource,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon,

TABLES_DEPENDED_ON

my $howToRestart = <<RESTART;
Use the Undo plugin first.
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

    my $resourceFile = $self->getArgs()->{resourceFile};

    open(FILE, $resourceFile) || $self->userError("can't open resource file '$resourceFile'");

    while(<FILE>) {
	chomp;
	my $resource = $self->parseResourceLine($_);
	if ($resource) {
	    $resource->submit();
	}
    }

    return "Done adding resources.";
}

sub parseResourceLine {
    my ($self, $line) = @_;
    
    my $resource;
    my @resData = split('\t',$line);
    
    my $dbh = $self->getQueryHandle();
    my $sql = "SELECT orthomcl_taxon_id
               FROM ApiDB.OrthomclTaxon
               WHERE three_letter_abbrev = ?";
    
    my $stmt = $dbh->prepare($sql);
    
    if (scalar @resData >= 11 && length($resData[1]) == 3) {
	    $resource = GUS::Model::ApiDB::OrthomclResource->new();
	    my ($taxonId) = $self->getTaxonId($stmt, $resData[1]);
	    $resource->setOrthomclTaxonId($taxonId);
	    $resource->setResourceName($resData[6]);
	    $resource->setResourceUrl($resData[8]);
	    $resource->setResourceVersion($resData[7]);
	    $resource->setStrain($resData[5]);
	    $resource->setDescription($resData[3]);
	    if (scalar @resData > 12) {
		$resource->setLinkoutUrl($resData[12]);
	    }
    }
    
    return $resource;
}

sub getTaxonId {
    my ($self, $stmt, $abbrev) = @_;
    
    my @id = $self->sqlAsArray( Handle => $stmt, Bind => [$abbrev] );
    
    if (scalar @id != 1) {
	$self->error("Should return one value for three_letter_abbrev '$abbrev'");
    }
    return @id;
}

sub undoTables {
    my ($self) = @_;
    
    return ('ApiDB.OrthomclResource',
	    );
}



1;
