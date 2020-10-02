#!/usr/bin/perl

use strict;

use lib $ENV{GUS_HOME} . "/lib/perl";

use Getopt::Long;
use File::Temp qw/ tempfile /;

use DBI;
use DBD::Oracle;

use CBIL::Util::PropertySet;

my ($help,$orthomclAbbrev,$proteomeFileName,$ecFileName,$downloadFileName,$logFileName,$projectName,$organismName);

&GetOptions('help|h' => \$help,
            'orthomclAbbrev=s' => \$orthomclAbbrev,
            'proteomeFileName=s' => \$proteomeFileName,
            'ecFileName=s' => \$ecFileName,
            'projectName=s' => \$projectName,
            'downloadFileName=s' => \$downloadFileName,
            'logFileName=s' => \$logFileName,
            'organismName=s' => \$organismName,
            );

foreach($orthomclAbbrev,$ecFileName,$proteomeFileName,$projectName,$organismName,$downloadFileName,$logFileName) {
  unless(defined $_) {
    &usage();
    die "proteome file name, ec file name, download file name, log file name, project name, organism name, and orthomcl abbrev are all required";
  }
}

my $baseUrl = getBaseUrl($projectName);
my $postText = getPostText($organismName,$projectName);
my $wgetCmd = getWgetCmd($baseUrl,$postText,$downloadFileName,$logFileName);
print "$wgetCmd\n\n";
system($wgetCmd);
writeFiles($orthomclAbbrev,$downloadFileName,$proteomeFileName,$ecFileName);

sub getBaseUrl {
    my ($projectName) = @_;

    $projectName = lc($projectName);
    my $url = "https://";

    my %projects = (
        micro => "microsporidiadb.org/micro",
        toxo => "toxodb.org/toxo",
        amoeba => "amoebadb.org/amoeba",
        crypto => "cryptodb.org/cryptodb",
        fungi => "fungidb.org/fungidb",
        giardia => "giardiadb.org/giardiadb",
	piroplasma => "piroplasmadb.org/piro",
	plasmo => "plasmodb.org/plasmo",
	trich => "trichdb.org/trichdb",
	tritryp => "tritrypdb.org/tritrypdb",
	host => "hostdb.org/hostdb",
	schisto => "schistodb.net/schisto",
	vectorbase => "vectorbase.org/vectorbase",
    );

    foreach my $project (keys %projects) {
	if ($projectName =~ /$project/) {
	    $url .= $projects{$project};
	    last;
	}
    }
    die "This is not a valid project name: '$projectName'\n" if ($url eq "https://");

    $url .= "/service/record-types/transcript/searches/GenesByGeneModelChars/reports/attributesTabular";

    return $url;
}


sub getPostText {
    my ($organismName,$projectName) = @_;

    my $coding = "protein coding";
    if (lc($projectName) =~ /vectorbase/) {
	$coding = "protein coding gene";
    }

    my $postText = "'{\"searchConfig\": {\"parameters\": { \"gene_or_transcript\": \"Genes\",\"gene_model_char\": \"{\\\"filters\\\":[{\\\"field\\\":\\\"organism\\\",\\\"type\\\":\\\"string\\\",\\\"isRange\\\":false,\\\"value\\\":[\\\"$organismName\\\"],\\\"includeUnknown\\\":false,\\\"fieldDisplayName\\\":\\\"Organism\\\"},{\\\"field\\\":\\\"gene_type\\\",\\\"type\\\":\\\"string\\\",\\\"isRange\\\":false,\\\"value\\\":[\\\"$coding\\\"],\\\"includeUnknown\\\":false,\\\"fieldDisplayName\\\":\\\"Gene Type\\\"}]}\"},\"wdkWeight\":10},\"reportConfig\": {\"attributes\": [\"primary_key\",\"source_id\",\"gene_product\",\"protein_sequence\",\"ec_numbers\"],\"includeHeader\": true,\"attachmentType\": \"text\",\"applyFilter\": false}}'";

    return $postText;
}


sub getWgetCmd {
    my ($baseUrl,$postText,$downloadFileName,$logFileName) = @_;

    my $cmd = "wget --output-file=$logFileName --output-document=$downloadFileName --post-data $postText --header 'content-type: application/json' \"$baseUrl\"";

    return $cmd;
}


sub writeFiles {
    my ($orthomclAbbrev,$downloadFileName,$proteomeFileName) = @_;

    open(IN, $downloadFileName)	or die "cannot open download file '$downloadFileName': $!";
    open(PROT, ">", $proteomeFileName) or die "cannot open proteome file '$proteomeFileName' for writing: $!";
    open(EC, ">", $ecFileName) or die "cannot open EC file '$ecFileName' for writing: $!";

    my $header = <IN>;

    while (<IN>) {
	my $line =$_;
	chomp $line;
	my @row = split("\t",$line);
	my ($gene,$tx,$prod,$seq,$ec) = ($row[0],$row[1],$row[2],$row[3],$row[4]);
	print PROT ">$tx gene=$gene product=$prod\n$seq\n";

	my @multipleEcs = split(/;/,$ec);
	foreach my $ecStr (@multipleEcs) {
	    if ($ecStr =~ /^([0-9\-\.]+)/) {
		my $singleEc = $1;
		print EC "$orthomclAbbrev|$gene\t$singleEc\n";
	    }
	}
    }

    close EC;
    close PROT;
}


sub usage {
  print "orthoGetProteinsEcFromVeupath.pl --orthomclAbbrev=s --ecFileName=s --proteomeFileName=s --downloadFileName=s --logFileName=s --projectName=s --organismName=s\n";
}

1;
