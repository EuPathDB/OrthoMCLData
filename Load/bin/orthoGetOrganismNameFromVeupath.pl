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
    die "data directory is required";
  }
}

my $baseUrls = getBaseUrls();
my $postText = getPostText();
my $numberFiles = runWgetCmds($baseUrls,$postText,$dataDir);

print "Saved $numberFiles files to $dataDir\n";

exit;


sub getBaseUrls {
    my $first = "https://";
    my $last = "/service/record-types/organism/searches/GeneMetrics/reports/attributesTabular";

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
	vectorbasedb => "vectorbase.org/vectorbase",
    );

    foreach my $project (keys %projects) {
	$projects{$project} = $first.$projects{$project}.$last;
    }

    return \%projects;
}


sub getPostText {
    my $postText = "'{\"searchConfig\": {\"parameters\": {},\"wdkWeight\": 10},\"reportConfig\": {\"attributes\": [\"primary_key\",\"name_for_filenames\",\"orthomcl_abbrev\"],\"includeHeader\": true,\"attachmentType\": \"text\"}}'";

    return $postText;
}


sub runWgetCmds {
    my ($baseUrls,$postText,$dataDir) = @_;
    
    foreach my $project (keys %{$baseUrls}) {
	my $downloadFile = $dataDir."/".$project.".txt";
	my $logFile = $dataDir."/".$project."_wget.log";
	my $cmd = "wget --output-file=$logFile --output-document=$downloadFile --post-data $postText --header 'content-type: application/json' \"$baseUrls->{$project}\"";

	print "$cmd\n\n";
	system($cmd);
    }

    my @files = glob($dataDir."/*.txt");
    print $_."\n" foreach (@files);
    return scalar @files;
}


sub usage {
  print "orthoGetOrganismNameFromVeupath.pl --dataDir=s --logFileName=s\n";
}

1;
