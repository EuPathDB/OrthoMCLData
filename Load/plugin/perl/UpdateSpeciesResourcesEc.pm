package OrthoMCLData::Load::Plugin::UpdateSpeciesResourcesEc;

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
Insert proteome source, format Ec file, and update organism name, all obtained from VEuPathDB sites.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert proteome source, format Ec file, and update organism name, all obtained from VEuPathDB sites.
PURPOSE_BRIEF

my $notes = <<NOTES;

NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclResource,
ApiDB.OrthomclTaxon
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthomclTaxon,
Sres.ExternalDatabase,
Sres.ExternalDatabaseRelease,
Sres.Taxon
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

    my $dataDir = $self->getArg('dataDir');

    my $speciesFromOrtho = $self->getSpeciesFromOrtho();
    my $speciesFromOrtho = $self->updateUniprotData($speciesFromOrtho,$dataDir);
    my $speciesFromOrtho = $self->updateVeupathData($speciesFromOrtho,$dataDir);
    my $speciesFromOrtho = $self->cleanUpData($speciesFromOrtho);

    my $numRows = $self->loadOrthoResource($speciesFromOrtho);
    $self->log("Finished adding to ApiDB.OrthomclResource. Loaded $numRows rows.\n");

    $numRows = $self->updateOrthoTaxon($speciesFromOrtho);
    $self->log("Finished updating ApiDB.OrthomclTaxon. Updated $numRows rows.\n");

    my $ecFileName = "ecFromVeupath.txt";
    my $numEcFiles = formatEcFile($dataDir,$ecFileName);
    $self->log("Used $numEcFiles EC files obtained from Veupath to make $dataDir/$ecFileName.\n");
}

sub getSpeciesFromOrtho {
    my ($self) = @_;

    my $sql = <<SQL;
SELECT ot.three_letter_abbrev,ot.orthomcl_taxon_id,ot.name,t.ncbi_tax_id
FROM apidb.orthomcltaxon ot, sres.taxon t
WHERE ot.core_peripheral IN ('C','P') AND ot.taxon_id=t.taxon_id
SQL
 
    my $dbh = $self->getQueryHandle();
    my $sth = $dbh->prepareAndExecute($sql);

    my $species;
    while (my @row = $sth->fetchrow_array()) {
	$species->{$row[0]}->{orthomclId} = $row[1];
	$species->{$row[0]}->{name} = $row[2];
	$species->{$row[0]}->{ncbiTaxId} = $row[3];
    }

    $sql = <<SQL;
SELECT ed.name, edr.version, edr.id_url
FROM Sres.ExternalDatabase ed,
     Sres.ExternalDatabaseRelease edr
WHERE (ed.name like '%orthomcl%Proteome_RSRC'
          OR ed.name like '%PeripheralFrom%'
          OR ed.name like '%CoreFrom%')
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

sub updateUniprotData {
    my ($self,$species,$dataDir) = @_;

    my $file = "$dataDir/UniprotProteomes";
    open(IN,$file) || die "Can't open file '$file'\n";
    my $uniprot;
    while (my $line = <IN>) {
	next unless ($line =~ /^UP/);
	chomp $line;
	my @fields = split("\t",$line);
	$uniprot->{$fields[1]}->{proteomeId} = $fields[0];
	$uniprot->{$fields[1]}->{name} = $fields[7];
    }
    close IN;

    foreach my $abbrev (keys %{$species}) {
	next unless (lc($species->{$abbrev}->{url}) =~ /uniprot/);
	$species->{$abbrev}->{resource}="Uniprot";
	my $proteomeId = "";
	$proteomeId = $uniprot->{$species->{$abbrev}->{ncbiTaxId}}->{proteomeId} if (exists $uniprot->{$species->{$abbrev}->{ncbiTaxId}}->{proteomeId});
	my $url = "https://www.uniprot.org/proteomes/".$proteomeId;
	$species->{$abbrev}->{url} = $url;
	$species->{$abbrev}->{name} = $uniprot->{$species->{$abbrev}->{ncbiTaxId}}->{name} if (exists $uniprot->{$species->{$abbrev}->{ncbiTaxId}}->{name});
    }

    return $species;
}

sub updateVeupathData {
    my ($self,$species,$dataDir) = @_;

    my @files = glob("$dataDir/*_organisms.txt");

    my $veupath;
    foreach my $file (@files) {
	open(IN,$file) || die "Can't open file '$file'\n";
	my $resource;
	if ($file =~ /\/([A-Za-z]+)_organisms\.txt/) {
	    $resource = $1;
	} else {
	    die "Did not find project name in file name: $file\n";
	}
	while (my $line = <IN>) {
	    chomp $line;
	    $line =~ s/<i>//g;
	    $line =~ s/<\/i>//g;
	    next if ($line =~ /^Organism/);
	    next unless ($line =~ /^[A-Za-z]/);
	    my @fields = split("\t",$line);
	    $veupath->{$fields[2]}->{name} = $fields[0];
	    $veupath->{$fields[2]}->{filename} = $fields[1];
	    $veupath->{$fields[2]}->{resource} = $resource;
	}
	close IN;	
    }

    foreach my $abbrev (keys %{$species}) {
	next unless (exists $veupath->{$abbrev});
	$species->{$abbrev}->{resource} = $veupath->{$abbrev}->{resource};
	$species->{$abbrev}->{name} = $veupath->{$abbrev}->{name};
	$species->{$abbrev}->{url} = getVeupathUrl($species->{$abbrev}->{resource},$veupath->{$abbrev}->{filename});
    }
    return $species;
}

sub getVeupathUrl {
    my ($resource,$filename) = @_;
    my $url = "https://";

    my %projects = (
        microsporidiadb => "microsporidiadb.org/micro",
        toxodb => "toxodb.org/toxo",
        amoebadb => "amoebadb.org/amoeba",
        cryptodb => "cryptodb.org/cryptodb",
        fungidb => "fungidb.org/fungidb",
        giardiadb => "giardiadb.org/giardiadb",
	piroplasmadb => "piroplasmadb.org/piro",
	plasmodb => "plasmodb.org/plasmo",
	trichdb => "trichdb.org/trichdb",
	tritrypdb => "tritrypdb.org/tritrypdb",
	hostdb => "hostdb.org/hostdb",
	schistodb => "schistodb.net/schisto",
	vectorbase => "vectorbase.org/vectorbase"
     );
    
    if ( exists $projects{lc($resource)} ) {
	$url .= $projects{lc($resource)}."/app/downloads/Current_Release/$filename/fasta/data/";
    } else {
	$url = "";
    }
    
    return $url;
}

sub cleanUpData {
    my ($self,$species) = @_;

    foreach my $abbrev (keys %{$species}) {
	if ( ! exists $species->{$abbrev}->{resource} ) {
	    my $abbrevWithoutOld = $abbrev;
	    $abbrevWithoutOld =~ s/-old//;
	    if ( exists $species->{$abbrevWithoutOld}->{resource} ) {
		$species->{$abbrev}->{resource} = $species->{$abbrevWithoutOld}->{resource};
		my $url = $species->{$abbrevWithoutOld}->{url};
		if ($url =~ /^(.+\/app\/downloads\/)/) {
		    $species->{$abbrev}->{url} = $1;
		}
		if ($species->{$abbrev}->{name} =~ /.+ (\(old build.+\))$/) {
		    $species->{$abbrev}->{name} = $species->{$abbrevWithoutOld}->{name}." ".$1;
		}
	    } elsif (exists $species->{$abbrev}->{url}) {
		if ( $species->{$abbrev}->{url} =~ /.+\.([A-Za-z]+)\.(org|net)/ ) {
		    my $resource = $1;
		    $resource = "VectorBase" if (lc($resource) eq "vectorbase");
		    $species->{$abbrev}->{resource} = $resource;
		    my $url = getVeupathUrl($resource);
		    if ($url ne "") {  #this a veupath url
			if ( $url =~ /^(.+\/app\/downloads\/)/ ) {
			    $species->{$abbrev}->{url} = $1;
			}
		    }
		} else {
		    $species->{$abbrev}->{resource} = "See URL";
		}
	    } else {
		$species->{$abbrev}->{resource} = "unknown";
		$species->{$abbrev}->{url} = "unknown";
	    }
	} else {
	    if ( ! exists $species->{$abbrev}->{url} ) {
		$species->{$abbrev}->{url} = "See Resource";
	    }
	}
    }
    return $species;
}

sub loadOrthoResource {
    my ($self, $species) = @_;

    my $sql = "SELECT orthomcl_taxon_id FROM apidb.orthomclresource";
    my $dbh = $self->getQueryHandle();
    my $sth = $dbh->prepareAndExecute($sql);
    my $numPast=0;
    while (my @row = $sth->fetchrow_array()) {
	$numPast++;
    }
    if ( $numPast > 0) {
	$self->log("There are $numPast rows in ApiDB.OrthomclResource. This table should be empty.\n");
    }

    my $numRows=0;
    foreach my $abbrev (keys %{$species}) {
	my $resource = $species->{$abbrev}->{resource};
	my $id = $species->{$abbrev}->{orthomclId};
	my $url = $species->{$abbrev}->{url};
	my $version = $species->{$abbrev}->{version};
	my $name = $species->{$abbrev}->{name};
	if (! $resource || ! $id || ! $url || ! $version) {
	    $self->log("incomplete record:\nabbrev: '$abbrev'\nresource '$resource'\nid '$id'\nurl '$url'\nversion '$version'\nname: '$name'\n");
	    next;
	}
	my $res = GUS::Model::ApiDB::OrthomclResource->new();
	$res->set('orthomcl_taxon_id', $id);
	$res->set('resource_name', $resource);
	$res->set('resource_url', $url);
	$res->set('resource_version', $version);
	$numRows += $res->submit();
	$res->undefPointerCache();
    }
    
    return $numRows;
}


sub updateOrthoTaxon {
    my ($self,$species) = @_;
    
    my $numRows=0;
    foreach my $abbrev (keys %{$species}) {
	my $taxon = GUS::Model::ApiDB::OrthomclTaxon->
	    new({three_letter_abbrev => $abbrev
		});

	$taxon->retrieveFromDB();

	if ($taxon->get('name') ne $species->{$abbrev}->{name}) {
	    $taxon->set('name', $species->{$abbrev}->{name});
	}
	$numRows += $taxon->submit();
	$self->undefPointerCache();
    }

    return $numRows;
}


sub formatEcFile {
    my ($dataDir,$ecFileName) = @_;

    my @files = glob("$dataDir/*_ec.txt");
    my $numEcFiles= scalar @files;

    open(OUT,">","$dataDir/$ecFileName") || die "Can't open file '$dataDir/$ecFileName' for writing\n";
    foreach my $file (@files) {
	open(IN,$file) || die "Can't open file '$file'\n";

	my $abbrev="";
	if ($file =~ /\/([A-Za-z]+)_ec\.txt/) {
	    $abbrev = $1;
	} else {
	    die "Did not find orthomcl abbrev in file name: $file\n";
	}

	my $header = <IN>;
	while (my $line = <IN>) {
	    chomp $line;
	    my @row = split("\t",$line);
	    my ($gene,$tx,$ec) = ($row[0],$row[1],$row[2]);

	    my @multipleEcs = split(/;/,$ec);
	    foreach my $ecStr (@multipleEcs) {
		if ($ecStr =~ /^([0-9\-\.]+)/) {
		    my $singleEc = $1;
		    print OUT "$abbrev|$gene\t$singleEc\n";
		}
	    }
	}
	close IN;
    }
    close OUT;
    return $numEcFiles;
}


sub undoTables {
    my ($self) = @_;

    return (
	    );
}


sub undoPreprocess {
    my ($self, $dbh, $rowAlgInvocationList) = @_;
    my $rowAlgInvocations = join(',', @{$rowAlgInvocationList});

    my $sql = "TRUNCATE TABLE ApiDB.OrthomclResource";
    my $sh = $dbh->prepare($sql);
    $sh->execute();
    $sh->finish();
}

1;
