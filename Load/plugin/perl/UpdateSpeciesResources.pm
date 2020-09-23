package OrthoMCLData::Load::Plugin::UpdateSpeciesResources;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use GUS::Model::ApiDB::OrthomclResource;
use GUS::Model::ApiDB::OrthomclTaxon;
use FileHandle;
use Data::Dumper;

my $argsDeclaration =
[

 stringArg({ descr => 'directory that contains the files downloaded from Veupath sites',
	          name  => 'dataDir',
	          isList    => 0,
	          reqd  => 1,
	          constraintFunc => undef,
	   }),

];


my $purpose = <<PURPOSE;
Insert proteome source and organism name, obtained from VEuPathDB sites.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert proteome source and organism name, obtained from VEuPathDB sites.
PURPOSE_BRIEF

my $notes = <<NOTES;

NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclResource,
ApiDB.OrthomclTaxon,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon,
Sres.ExternalDatabase,
Sres.ExternalDatabaseRelease,
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

    my $dataDir = uc($self->getArg('dataDir'));


    my $speciesFromOrtho = $self->getSpeciesFromOrtho();
    my $numRows = $self->loadRows($species,$proteomesFromBuild);
    $self->log("Finished adding to ApiDB.OrthomclResource. Loaded $numRows rows.\n");
}

sub getSpeciesFromOrtho {
    my ($self) = @_;


    my $sql = <<SQL;
SELECT three_letter_abbrev,orthomcl_taxon_id
FROM apidb.orthomcltaxon
WHERE core_peripheral IN ('C','P')
SQL
 
    my $dbh = $self->getQueryHandle();
    my $sth = $dbh->prepareAndExecute($sql);

    my $species;
    while (my @row = $sth->fetchrow_array()) {
	$species->{$row[0]}->{id} = $row[1];
    }

    $sql = <<SQL;
SELECT ed.name, edr.version, edr.id_url
FROM Sres.ExternalDatabase ed,
     Sres.ExternalDatabaseRelease edr
WHERE ed.name like '%orthomcl%Proteome_RSRC'
    AND ed.external_database_id = edr.external_database_id
SQL
 
    $sth = $dbh->prepareAndExecute($sql);

    while (my @row = $sth->fetchrow_array()) {
	my @array = split(/_/, $row[0]);
	my $currentAbbrev = shift @array;
	if (! exists $species->{$currentAbbrev} ) {
	    $self->error("Abbreviation '$currentAbbrev' not in orthomcltaxon table.\n");
	}
	$species->{$currentAbbrev}->{version} = $row[1];
	$species->{$currentAbbrev}->{url} = $row[2];
    }
    
    foreach my $abbrev (keys %{$species}) {
	if (! exists $species->{$abbrev}->{version} ) {
	    $self->error("Abbreviation '$abbrev' does not have version in ExtDb or ExtDbRls tables.\n");
	}
	if (! exists $species->{$abbrev}->{url} ) {
	    $self->error("Abbreviation '$abbrev' does not have url in ExtDb or ExtDbRls tables.\n");
	}

    }

    return $species;
}


sub loadRows {
    my ($self, $species, $proteomesFromBuild) = @_;

# make sure there are no or only 1 of each taxon_id
# if none, add it, if one, then modify it

    my $sql = <<SQL;
SELECT orthomcl_taxon_id
FROM apidb.orthomclresource
SQL
 
    my $dbh = $self->getQueryHandle();
    my $sth = $dbh->prepareAndExecute($sql);

    my $numRows=0;
    my $numPast=0;
    my %pastResources;
    while (my @row = $sth->fetchrow_array()) {
	$numPast++;
	if (exists $pastResources{$row[0]} ) {
	    $self->error("More than one row for orthomcl_taxon_id '$row[0]' in apidb.orthomclresource\n");
	}
	$pastResources{$row[0]}=1;
    }

    if ( $numPast == 0) {
	$self->log("There are no rows in ApiDB.OrthomclResource. Adding rows.\n");
    } else {
	$self->log("There are $numPast rows in ApiDB.OrthomclResource. Adding rows for new proteomes.\n");
    }

    my $projects = getProjects($proteomesFromBuild);

    foreach my $abbrev (keys %{$species}) {
	my $id = $species->{$abbrev}->{id};
	next if (exists $pastResources{$id});
	my $version = $species->{$abbrev}->{version};
	my $resourceName = getResourceNameFromUrl($species->{$abbrev}->{url});
	my $formattedResourceName=$resourceName;
	my $url=$resourceName;
	if (exists $projects->{$resourceName}) {
	    $formattedResourceName = $projects->{$resourceName}->{formatted};
	    $url = $projects->{$resourceName}->{url};
	}
	    
	my $resource = GUS::Model::ApiDB::OrthomclResource->new({'orthomcl_taxon_id'=>$id});
	$resource->retrieveFromDB();
	if ($resource->get('resource_name') ne $formattedResourceName ) {
	    $resource->set('resource_name', $formattedResourceName);
	}
	if ($resource->get('resource_url') ne $url ) {
	    $resource->set('resource_url', $url);
	}
	if ($resource->get('resource_version') ne $version ) {
	    $resource->set('resource_version', $version);
	}
	$numRows += $resource->submit();
	$resource->undefPointerCache();
    }

    return $numRows;
}

sub getResourceNameFromUrl {
    my ($url) = @_;
    $url = lc($url);
    if ( $url =~ /[^a-z]([a-z]+)\.[no][er][tg]/ ) {
	my $resource = $1;
	return $resource;
    } else {
	return $url;
    }
}

sub getProjects {
    my ($proteomesFromBuild) = @_;
    my @projectsLc = qw/amoebadb cryptodb fungidb giardiadb hostdb microsporidiadb piroplasmadb plasmodb schistodb toxodb trichdb tritrypdb vectorbase uniprot/;
    my @projectsCaps = qw/AmoebaDB CryptoDB FungiDB GiardiaDB HostDB MicrosporidiaDB PiroplasmaDB PlasmoDB SchistoDB ToxoDB TrichDB TriTrypDB VectorBase Uniprot/;
    my %projects;

    foreach my $project (@projectsLc) {
	$projects{$project}->{formatted} = shift @projectsCaps;
	if ($project eq "uniprot") {
	    $projects{$project}->{url} = "https://www.uniprot.org/proteomes/";
	} elsif ($project eq "schistodb") {
	    $projects{$project}->{url} = "https://schistodb.net/common/downloads/release-".$proteomesFromBuild."/";
	} else {
	    $projects{$project}->{url} = "https://".$project.".org/common/downloads/release-".$proteomesFromBuild."/";
	}
    }

    return \%projects;
}

sub undoTables {
    my ($self) = @_;

    return (
	ApiDB.OrthomclResource
	    );
}



1;
