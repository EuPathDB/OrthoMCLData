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
The resourceFile is the proteome file with the following tab delimited columns:
  3 letter species abrev
  ncbi tax id
  organism name
  data source name abbreviated (e.g. GenBank or JGI)
  URL to get file

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

    my $total;

    my $resourceFile = $self->getArgs()->{resourceFile};

    open(FILE, $resourceFile) || $self->userError("can't open resource file '$resourceFile'");

    while(<FILE>) {
      next if ($_ =~ /NAME/ || $_ =~ /^$/);
      chomp;
      my $total += $self->parseResourceLine($_);
    }

    return "Done adding resources. Loaded $total rows.";
}

sub parseResourceLine {
    my ($self, $line) = @_;

    my $num;

    my @resData = split('\t',$line);

    my $dbh = $self->getQueryHandle();
    my $sql = "SELECT orthomcl_taxon_id
               FROM ApiDB.OrthomclTaxon
               WHERE three_letter_abbrev = ?";

    my $stmt = $dbh->prepare($sql);

    my ($taxonId) = $self->getTaxonId($stmt, $resData[0]);

    my $resource = GUS::Model::ApiDB::OrthomclResource->new({'orthomcl_taxon_id'=>$taxonId,
                                                          'resource_name'=>$resData[3],
                                                          'resource_url'=>$resData[4],
                                                          'strain' => $resData[2]});
    unless ($resource->retrieveFromDB()) {
      $num = $resource->submit();
      $resource->undefPointerCache();
    }

    return $num;
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
