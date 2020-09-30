#!/usr/bin/perl

use strict;

use lib $ENV{GUS_HOME} . "/lib/perl";

use Getopt::Long;
use File::Temp qw/ tempfile /;

use DBI;
use DBD::Oracle;

use CBIL::Util::PropertySet;

my ($help,$dataDir);

&GetOptions('help|h' => \$help,
            'dataDir=s' => \$dataDir,
            );

foreach($dataDir) {
  unless(defined $_) {
    &usage();
    die "data directory is required\n";
  }
}

my $organismBaseUrls = getBaseUrls("organism");
my $organismPostText = getPostText("organism","");
my $organismFiles = runOrganismWgetCmds($organismBaseUrls,$organismPostText,$dataDir);

my $ecBaseUrls = getBaseUrls("ec");
my $ecFiles = runEcWgetCmds($ecBaseUrls,$organismFiles,$dataDir);

exit;


sub getBaseUrls {
    my ($type) = @_;

    my $first = "https://qa.";

    my $last;
    if ($type eq "organism") {
	$last = ".b49/service/record-types/organism/searches/GeneMetrics/reports/attributesTabular";
    } elsif ($type eq "ec") {
	$last = ".b49/service/record-types/transcript/searches/GenesByEcNumber/reports/attributesTabular";
    } else {
	die "Type must be 'organism' or 'ec' for getBaseUrl.\n";
    }

    my %projects = (
        MicrosporidiaDB => "microsporidiadb.org/micro",
        ToxoDB => "toxodb.org/toxo",
        AmoebaDB => "amoebadb.org/amoeba",
        CryptoDB => "cryptodb.org/cryptodb",
        FungiDB => "fungidb.org/fungidb",
        GiardiaDB => "giardiadb.org/giardiadb",
    	PiroplasmaDB => "piroplasmadb.org/piro",
    	PlasmoDB => "plasmodb.org/plasmo",
    	TrichDB => "trichdb.org/trichdb",
    	TriTrypDB => "tritrypdb.org/tritrypdb",
    	HostDB => "hostdb.org/hostdb",
    	SchistoDB => "schistodb.net/schisto",
    	VectorBase => "vectorbase.org/vectorbase",
    );

    foreach my $project (keys %projects) {
	$projects{$project} = $first.$projects{$project}.$last;
    }

    return \%projects;
}


sub getPostText {
    my ($type,$organismName) = @_;

    my $postText;
    if ($type eq "organism") {
	$postText = "'{\"searchConfig\": {\"parameters\": {},\"wdkWeight\": 10},\"reportConfig\": {\"attributes\": [\"primary_key\",\"name_for_filenames\",\"orthomcl_abbrev\"],\"includeHeader\": true,\"attachmentType\": \"text\"}}'";
    } elsif ($type eq 'ec') {
	$postText = "'{\"searchConfig\": {\"parameters\": {\"organism\": \"[\\\"$organismName\\\"]\",\"ec_source\": \"[\\\"GeneDB\\\",\\\"GenBank\\\",\\\"MPMP\\\",\\\"Sanger\\\",\\\"Uniprot\\\"]\",\"ec_number_pattern\": \"N/A\",\"ec_wildcard\": \"*\"},\"wdkWeight\": 10},\"reportConfig\": {\"attributes\": [\"primary_key\",\"source_id\",\"ec_numbers\"],\"includeHeader\": true,\"attachmentType\": \"text\",\"applyFilter\": false}}'";
    } else {
	die "Type must be 'organism' or 'ec' for getPostText.\n";
    }

    return $postText;
}


sub runOrganismWgetCmds {
    my ($baseUrls,$postText,$dataDir) = @_;

    # one file for each genomic project
    my %organismFiles;
    foreach my $project (keys %{$baseUrls}) {
	my $downloadFile = $dataDir."/".$project."_organisms.txt";
	$organismFiles{$project} = $downloadFile;
	my $logFile = $dataDir."/".$project."_organisms_wget.log";
	my $url = $baseUrls->{$project};
	my $cmd = "wget --output-file=$logFile --output-document=$downloadFile --post-data $postText --header 'content-type: application/json' \"$url\"";
	print "$cmd\n\n";
	system($cmd);
	die "Download file $downloadFile was not properly obtained with wget.\n" if (-s $downloadFile == 0);
    }

    # one file for uniprot proteomes
    my $cmd = "wget --output-file='$dataDir/uniprot_wget.log' --output-document=$dataDir/UniprotProteomes \"ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/reference_proteomes/README\"";
    print "$cmd\n\n";
    system($cmd);
    die "Download file $dataDir/UniprotProteomes was not properly obtained with wget.\n" if (-s "$dataDir/UniprotProteomes" == 0);

    return \%organismFiles;
}

sub runEcWgetCmds {
    my ($ecBaseUrls,$organismFiles,$dataDir) = @_;

    my $numEcFiles = 0;
    foreach my $project (keys %{$ecBaseUrls}) {
	my $organisms = readOrganismFile($organismFiles->{$project});
	foreach my $abbrev (keys %{$organisms} ) {
	    my $downloadFile = $dataDir."/".$abbrev."_ec.txt";
	    my $logFile = $dataDir."/".$abbrev."_ec_wget.log";
	    my $postText = getPostText("ec",$organisms->{$abbrev});
	    my $url = $ecBaseUrls->{$project};
	    my $cmd = "wget --output-file=$logFile --output-document=$downloadFile --post-data $postText --header 'content-type: application/json' \"$url\"";
	    print "$cmd\n\n";
	    system($cmd);
	    die "Download file $downloadFile was not properly obtained with wget.\n" if (-s $downloadFile == 0);
	    $numEcFiles++;
	}
    }
    return $numEcFiles;
}

sub readOrganismFile {
    my ($file) = @_;

    my %organisms;
    open(IN, $file) or die "cannot open download file '$file': $!";    
    while (my $line =<IN>) {
	chomp $line;
	$line =~ s/<i>//g;
	$line =~ s/<\/i>//g;
	next if ($line =~ /^Organism/);
	next unless ($line =~ /^[A-Za-z]/);
	my @fields = split("\t",$line); 
	$organisms{$fields[2]} = $fields[0];
    }

    close IN;
    return \%organisms;
}

sub usage {
  print "orthoGetOrganismNameFromVeupath.pl --dataDir=s";
}

1;
