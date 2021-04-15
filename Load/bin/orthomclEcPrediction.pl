#!/usr/bin/perl

use warnings;
use strict;
use Data::Dumper;
use lib "$ENV{GUS_HOME}/lib/perl";
use CBIL::Util::PropertySet;
use DBI;

my $outputDir = $ARGV[0];
my $organismDir = $ARGV[1];
my $version = $ARGV[2];

# example command line:  orthomclEcPrediction.pl /home/markhick/EC
# good groups to study: OG6_100435, OG6_101725

my $minNumProteinsWithEc = 1;    # if not testing, then set to 1
my $maxNumProteinsWithEc = 0;    # if not testing, then set to 0
my $minNumGeneraForViableEc = 1;
my $minNumProteinsForViableEc = 1;
my $excludeOld = 1;
my $createStatsFile = 0;       # if not testing, then set to 0
my $createProteinFile = 0;     # if not testing, then set to 0
my $createLogFile = 0;         # if not testing, then set to 0 and the log will go to STDERR
my $test = 0;                  # if not testing, then set to 0
my $fractionOfGroups = 1;      # if not testing, then set to 1

my ($testFraction,$totalEcsTested,$noEcMatch,$testExact,$testLessPrecise,$testMorePrecise,$numProteinsExtraEcRef,$numExtraEcRef,$numTotalProteinsRef,$testFh) = &setUpTest($outputDir) if ($test);

my ($logFh,$netGroupsRef,$dbh,$groups) = &setUpPrediction($outputDir,$fractionOfGroups,$minNumProteinsWithEc,$maxNumProteinsWithEc,$createLogFile);

my ($organisms,$fHs) = &getOrganismsProjectsFileHandles($organismDir,$outputDir,$version,$dbh,$excludeOld);

foreach my $group (keys %{$groups}) {
    next if (&nextGroup($fractionOfGroups,$netGroupsRef,$logFh));

    my ($statsFh,$proteinFile) = &makeGroupDirAndFiles($outputDir,$group,$createStatsFile,$createProteinFile);
    my ($proteinIds,$numTotalProteins,$numTotalGenera) = &getProteinInfo($group,$excludeOld,$dbh,$proteinFile);

    my ($trainingIds,$testIds);
    ($trainingIds,$testIds) = &getTrainingAndTestIds($proteinIds,$testFraction) if ($test);
    next if ($test && ($trainingIds eq "" || $testIds eq ""));

    my $viableEcNumbers = &getViableEcNumbers($proteinIds,$test,$trainingIds,$minNumGeneraForViableEc,$minNumProteinsForViableEc,$statsFh);
    
    my ($domainStatsPerEc,$domainPerProtein);
    if (&numProteinsWithDomainAndEc($proteinIds) > 0) {
	($domainStatsPerEc,$domainPerProtein) = &ecDomainStats($proteinIds,$test,$trainingIds,$viableEcNumbers,$statsFh);
    }
    my $lengthStatsPerEc = &ecLengthStats($proteinIds,$test,$trainingIds,$viableEcNumbers,$statsFh);
    my ($blastStatsPerEc,$blastStatsPerProtein) = &ecBlastStats($dbh,$group,$proteinIds,$test,$trainingIds,$viableEcNumbers,$excludeOld,$statsFh);
    close $statsFh if ($statsFh);

    my $scores =  &getProteinScores($proteinIds,$numTotalProteins,$numTotalGenera,$test,$testIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$organisms,$fHs);

     &testScores($scores,$proteinIds,$testIds,$totalEcsTested,$noEcMatch,$testExact,$testLessPrecise,$testMorePrecise,$numProteinsExtraEcRef,$numExtraEcRef,$numTotalProteinsRef,$testFh,$logFh) if ($test);
}

&printTest($totalEcsTested,$noEcMatch,$testExact,$testLessPrecise,$testMorePrecise,$numProteinsExtraEcRef,$numExtraEcRef,$numTotalProteinsRef,$testFh) if ($test);

&endPrediction($dbh,$logFh,$createLogFile,$fHs);

exit;




################################  SUBROUTINES  ########################################

sub getOrganismsProjectsFileHandles {
    my ($organismDir,$outputDir,$version,$dbh,$excludeOld) = @_;
    my $organisms = &getOrganisms($organismDir);
    &addOrganismsFromOrtho($organisms,$dbh,$excludeOld);
    my $fHs = &getFileHandles($organisms,$outputDir,$version);
    return ($organisms,$fHs);
}

sub getFileHandles {
    my ($organisms,$outputDir,$version) = @_;
    my $fHs;
    foreach my $abbrev (keys %{$organisms}) {
	if (! exists $organisms->{$abbrev}) {
	    print STDERR "Cannot find a project for '$abbrev'. To be safe, I will put this organism into all projects.\n";
	    next;
	}
	my $project = $organisms->{$abbrev};
	next if ($project eq "none" || exists $fHs->{$project});
	my $filePath = $outputDir;
	$filePath =~ s/PROJECT/$project/;
	$filePath =~ s/VERSION/$version/;
	&makeDir($filePath);
	print STDERR "Created directory $filePath\n";
	$filePath .= "/ec.txt";
	open(my $fh,">",$filePath) || die "Cannot open file '$filePath' for writing";
	print STDERR "Opened $fh for writing\n";
	$fHs->{$project} = $fh;
    }
    return $fHs;
}

sub getOrganisms {
    my ($organismDir) = @_;
    my $organisms;
    my @files = glob("$organismDir/*_organisms.txt");
    foreach my $file (@files) {
	my $project;
	if ($file =~ /\/([A-Za-z]+)_organisms\.txt/) {
	    $project = $1;
	} else {
	    die "Did not find project name in file '$file'\n";
	}
	open(IN,$file) || die "Can't open file '$file'\n";
	while (my $line = <IN>) {
	    chomp $line;
	    $line =~ s/<i>//g;
	    $line =~ s/<\/i>//g;
	    next if ($line =~ /^Organism/);
	    next unless ($line =~ /^[A-Za-z]/);
	    my @fields = split("\t",$line);
	    $organisms->{$fields[2]} = $project;
	    print STDERR "Abbrev '$fields[2]'  Project '$project'\n";
	}
	close IN;	
    }
    return $organisms;
}

sub addOrganismsFromOrtho {
    my ($organisms,$dbh,$excludeOld) = @_;
    my $cladeToProject = &getCladeToProject();
    my $query = $dbh->prepare(&orthoOrganismSql($excludeOld));
    $query->execute();
    while (my($abbrev,$clade) = $query->fetchrow_array()) {
	$abbrev = "rirr" if ($abbrev eq "rhiz");   # this is temporary, because rhiz on orthomcl equals rirr on fungidb 
	next if (exists $organisms->{$abbrev});   # already have this
	if (! exists $cladeToProject->{$clade}) {
	    print STDERR "Abbrev '$abbrev'  Project 'unknown'\n";
	    next if (! exists $cladeToProject->{$clade});  # can't find a project for this one
	}
	$organisms->{$abbrev} = $cladeToProject->{$clade};
	print STDERR "Abbrev '$abbrev'  Project '$cladeToProject->{$clade}'\n";
    }
    $query->finish();
}

sub setUpPrediction {
    my ($outputDirectory,$fractionOfGroups,$minNumProteinsWithEc,$maxNumProteinsWithEc,$createLogFile) = @_;
    my $dbh = &getDbHandle();
    my $groups = &getGroupsFromDatabase($minNumProteinsWithEc,$maxNumProteinsWithEc,$dbh);
        #$groups->{"OG6_500798"}=1;

    my $logFh;
    my $numGroups = keys %{$groups};
    my $netGroups = sprintf("%.0f",$fractionOfGroups * $numGroups);
    if ($createLogFile) {
	my $logFile = "$outputDirectory/log.txt";
	open($logFh,">",$logFile) || die "Cannot open file '$logFile' for writing";
    }
    print {$logFh ? $logFh : *STDERR } "Start time: ".localtime()."\n";
    print {$logFh ? $logFh : *STDERR } "Groups must have between $minNumProteinsWithEc and $maxNumProteinsWithEc proteins with EC numbers\n";
    print {$logFh ? $logFh : *STDERR } "Total groups: $numGroups  Fraction: $fractionOfGroups  Net number of groups: $netGroups\n";
    return ($logFh,\$netGroups,$dbh,$groups);
}

sub nextGroup {
    my ($fractionOfGroups,$netGroupsRef,$logFh) = @_;
    return 1 if (rand(1) >= $fractionOfGroups);
    print {$logFh ? $logFh : *STDERR } "Approx number groups remaining: ${$netGroupsRef}\n";
    ${$netGroupsRef}--;
    return 0;
}

sub endPrediction {
    my ($dbh,$logFh,$createLogFile,$fHs) = @_;
    $dbh->disconnect();
    print {$logFh ? $logFh : *STDERR } "End time: ".localtime()."\n";
    close $logFh if ($createLogFile);
    foreach my $project (keys %{$fHs}) {
	print $fHS->{$project} "completed\n";
	close $fHs->{$project};
    }
}

sub setUpTest {
    my ($outputDirectory) = @_;
    my $testFraction = 0.3;
    my $totalEcsTested=0;
    my $noEcMatch=0;
    my $numProteinsExtraEc=0;
    my $numExtraEc=0;
    my $numTotalProteins=0;
    my %testExact;
    my %testLessPrecise;
    my %testMorePrecise;
    my $testFile =  "$outputDirectory/test.txt";
    open(my $testFh,">",$testFile) || die "Cannot open $testFile for writing\n";
    return (\$testFraction,\$totalEcsTested,\$noEcMatch,\%testExact,\%testLessPrecise,\%testMorePrecise,\$numProteinsExtraEc,\$numExtraEc,\$numTotalProteins,$testFh);
}


sub getTrainingAndTestIds {
    my ($proteinIds,$testFraction) = @_;
    my ($trainingIds,$testIds);

    my $numProteinsWithEc = &numProteinsWithEc($proteinIds);
    return ("","") if $numProteinsWithEc < 2;
    my $uniqueEcNumbers = &getUniqueEcNumbersFromProteins($proteinIds,0,0);
    foreach my $uniqueEc (keys %{$uniqueEcNumbers}) {
	return ("","") if ($uniqueEcNumbers->{$uniqueEc} < 2);
    }
    my $numProteinsTest = sprintf("%.0f",${$testFraction} * $numProteinsWithEc);
    $numProteinsTest = 1 if ($numProteinsTest < 1);
    $numProteinsTest = $numProteinsWithEc - 1 if ($numProteinsTest == $numProteinsWithEc);
    my $testIdPositions = &getTestIdPositions($numProteinsTest,$numProteinsWithEc);

    my $proteinCounter = 1;
    foreach my $id (keys %{$proteinIds}) {
	next if (scalar @{$proteinIds->{$id}->{ec}} == 0);
	if (exists $testIdPositions->{$proteinCounter}) {
	    $testIds->{$id} = 1;
	} else {
	    $trainingIds->{$id} = 1;
	}
	$proteinCounter++;
    }
    return("","") if (&ecsNotInTrainingAndTest($proteinIds,$trainingIds,$testIds,$uniqueEcNumbers));
    return ($trainingIds,$testIds);
}

sub ecsNotInTrainingAndTest {
    my ($proteinIds,$trainingIds,$testIds,$uniqueEcNumbers) = @_;
    foreach my $uniqueEc (keys %{$uniqueEcNumbers}) {
	my $found=0;
	foreach my $id (keys %{$trainingIds}) {
	    foreach my $idEc (@{$proteinIds->{$id}->{ec}}) {
		if ($idEc eq $uniqueEc) {
		    $found=1;
		    last;
		}
	    }
	    last if ($found == 1);
	}
	return 1 if ($found == 0);
	foreach my $id (keys %{$testIds}) {
	    foreach my $idEc (@{$proteinIds->{$id}->{ec}}) {
		if ($idEc eq $uniqueEc) {
		    $found=2;
		    last;
		}
	    }
	    last if ($found == 2);
	}
	return 1 if ($found == 1);
    }
    return 0;
}


sub getTestIdPositions {
    my ($numProteinsTest,$numProteinsWithEc) = @_;
    my %testPositions;
    while (keys %testPositions < $numProteinsTest ) {
	my $randomNumber = int(rand($numProteinsWithEc)) + 1;
	next if (exists $testPositions{$randomNumber});
	$testPositions{$randomNumber} = 1;
    }
    return \%testPositions;
}

sub makeGroupDirAndFiles {
    my ($outputDirectory,$group,$createStatsFile,$createProteinFile) = @_;
    &makeDir("$outputDirectory/$group") if ($createStatsFile || $createProteinFile);
    my $statsFh;
    if ($createStatsFile) {
	my $statsFile = "$outputDirectory/$group/stats.txt";
	open($statsFh,">",$statsFile) || die "Cannot open $statsFile for writing\n";
    }
    my $proteinFile;
    if ($createProteinFile) {
	$proteinFile = "$outputDirectory/$group/proteins.txt";
    }
    return ($statsFh,$proteinFile);
}

sub ecBlastStats {
    my ($dbh,$group,$proteinIds,$test,$trainingIds,$viableEcNumbers,$excludeOld,$statsFh) = @_;

    my ($blastEvalues,$blastPerEc) = &readBlastEvaluesFromDatabase($dbh,$group,$proteinIds,$test,$trainingIds,$viableEcNumbers,$excludeOld,$statsFh);
    my @orderedViableEcs = sort keys %{$viableEcNumbers};
    my $blastStatsPerEc = &calculateBlastStatsPerEc(\@orderedViableEcs,$blastPerEc,$statsFh);
    my $blastStatsPerProtein = &calculateBlastStatsPerProtein(\@orderedViableEcs,$blastEvalues,$statsFh);
    
    return ($blastStatsPerEc,$blastStatsPerProtein);
}

sub readBlastEvaluesFromDatabase {
    my ($dbh,$group,$proteinIds,$test,$trainingIds,$viableEcNumbers,$excludeOld,$statsFh) = @_;

    my $blastEvalues;    # id -> ec -> (-5,-6.4,-150.3)
    my $blastPerEc;        # ec -> (-5,-6.4,-150.3)  only if both proteins have EC
    my %missing;
    my $query = $dbh->prepare(&blastSql($group));
    
    $query->execute();
    while (my($query,$subject,$mantua,$exponent) = $query->fetchrow_array()) {
	next if ($excludeOld && ($query =~ /-old\|/ || $subject =~ /-old\|/));
	next if (&proteinDoesNotExist($subject,$proteinIds,$statsFh,\%missing));
	next if (&proteinDoesNotExist($query,$proteinIds,$statsFh,\%missing));
	
	my $subjectHasEc = scalar @{$proteinIds->{$subject}->{ec}} > 0 ? 1 : 0;
	my $queryHasEc = scalar @{$proteinIds->{$query}->{ec}} > 0 ? 1 : 0;
	next if ( ! $subjectHasEc && ! $queryHasEc );
	
	my $exponentFromMantua = $mantua == 0 ? 0 : log($mantua)/log(10);
	$exponent += $exponentFromMantua;
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$query},$viableEc) && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		if (! $test || ($test && exists $trainingIds->{$query} && exists $trainingIds->{$subject})) {
		    $blastPerEc->{$viableEc} = &addArrayElement($blastPerEc->{$viableEc},$exponent);
		}
	    }
	    if ($subjectHasEc && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		if (! $test || ($test && exists $trainingIds->{$subject})) {
		    $blastEvalues->{$query}->{$viableEc} = &addArrayElement($blastEvalues->{$query}->{$viableEc},$exponent);
		}
	    }
	    if ($queryHasEc && &proteinHasThisEcNumber($proteinIds->{$query},$viableEc)) {
		if (! $test || ($test && exists $trainingIds->{$query})) {
		    $blastEvalues->{$subject}->{$viableEc} = &addArrayElement($blastEvalues->{$subject}->{$viableEc},$exponent);
		}
	    }
	}
    }
    $query->finish();

    return ($blastEvalues,$blastPerEc);
}

sub calculateBlastStatsPerEc {
    my ($orderedEcs,$blastPerEc,$statsFh) = @_;

    my $blastStatsPerEc;
    foreach my $ec (@{$orderedEcs}) {
	my $noValues;
	$noValues = 1 if (! exists $blastPerEc->{$ec});   # this EC does not have any blast partners
	$blastStatsPerEc->{$ec}->{numValues} = $noValues ? 0 : scalar @{$blastPerEc->{$ec}};
	$blastStatsPerEc->{$ec}->{min} = $noValues ? 0 : sprintf('%.1f',min($blastPerEc->{$ec}));
	$blastStatsPerEc->{$ec}->{max} = $noValues ? 0 : sprintf('%.1f',max($blastPerEc->{$ec}));
	$blastStatsPerEc->{$ec}->{median} = $noValues ? 0 : sprintf('%.1f',median($blastPerEc->{$ec}));
	my ($mean,$sd) = meanSd($blastPerEc->{$ec}) if (! $noValues);
	$blastStatsPerEc->{$ec}->{mean} = $noValues ? 0 : sprintf('%.1f',$mean);
	$blastStatsPerEc->{$ec}->{sd} = $noValues ? 0 : sprintf('%.1f',$sd);
    }
    
    &printBlastStatsPerEc($blastStatsPerEc,$statsFh) if ($statsFh);
    
    return $blastStatsPerEc;
}

sub printBlastStatsPerEc {
    my ($blastStatsPerEc,$statsFh) = @_;
    print $statsFh "\nBLAST_STATISTICS\n";
    print $statsFh "ec_number\tnum_values\tminimum\tmaximum\tmedian\tmean\tstd_dev\n";
    foreach my $ec (keys %{$blastStatsPerEc}) {
	print $statsFh "$ec";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{numValues}";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{min}";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{max}";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{median}";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{mean}";
	print $statsFh "\t$blastStatsPerEc->{$ec}->{sd}";
	print $statsFh "\n";
    }
    print $statsFh "\n";
}


sub calculateBlastStatsPerProtein {
    my ($orderedEcs,$blastEvalues,$statsFh) = @_;
    
    my $blastStatsPerProtein;
    foreach my $id (keys %{$blastEvalues}) {
	foreach my $viableEc ( @{$orderedEcs} ) {
	    my $noValues;
	    $noValues = 1 if (! exists $blastEvalues->{$id}->{$viableEc});   # this id does not have blast partner containing this EC number
	    $blastStatsPerProtein->{$id}->{$viableEc}->{numValues} = $noValues ? 0 : scalar @{$blastEvalues->{$id}->{$viableEc}};
	    $blastStatsPerProtein->{$id}->{$viableEc}->{min} = $noValues ? 0 : sprintf('%.1f',min($blastEvalues->{$id}->{$viableEc}));
	    $blastStatsPerProtein->{$id}->{$viableEc}->{max} = $noValues ? 0 : sprintf('%.1f',max($blastEvalues->{$id}->{$viableEc}));
	    $blastStatsPerProtein->{$id}->{$viableEc}->{median} = $noValues ? 0 : sprintf('%.1f',median($blastEvalues->{$id}->{$viableEc}));
	    my ($mean,$sd) = meanSd($blastEvalues->{$id}->{$viableEc}) if (! $noValues);
	    $blastStatsPerProtein->{$id}->{$viableEc}->{mean} = $noValues ? 0 : sprintf('%.1f',$mean);
	    $blastStatsPerProtein->{$id}->{$viableEc}->{sd} = $noValues ? 0 : sprintf('%.1f',$sd);
	}
    }

    &printBlastStatsPerProtein($orderedEcs,$blastStatsPerProtein,$statsFh) if ($statsFh);
    
    return $blastStatsPerProtein;
}

sub printBlastStatsPerProtein {
    my ($orderedEcs,$blastStatsPerProtein,$statsFh) = @_;
    print $statsFh "GENE\t";
    foreach my $ec (@{$orderedEcs}) {
	print $statsFh "$ec\t\t\t\t\t\t";
    }
    print $statsFh "\n";
    foreach my $ec (@{$orderedEcs}) {
	print $statsFh "\tnumber_values\tminimum\tmaximum\tmedian\tmean\tstd_dev";
    }
    print $statsFh "\n";
    foreach my $id (keys %{$blastStatsPerProtein}) { 
	print $statsFh "$id";
	foreach my $viableEc ( @{$orderedEcs} ) {
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{numValues}";
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{min}";
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{max}";
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{median}";
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{mean}";
	    print  $statsFh "\t$blastStatsPerProtein->{$id}->{$viableEc}->{sd}";
	}
	print $statsFh "\n";
    }
    print $statsFh "\n";
}

sub proteinDoesNotExist {
    my ($proteinName,$proteinIds,$statsFh,$missingHashRef) = @_;

    if (! exists $proteinIds->{$proteinName}) {
	if ($statsFh && ! exists $missingHashRef->{$proteinName}) {
	    print $statsFh "ERROR: The protein '$proteinName' does not exist in the original protein ids\n";
	    $missingHashRef->{$proteinName}=1;
	}
	return 1;
    }
    return 0;
}

sub numProteinsWithDomainAndEc {
    my ($proteinIds) = @_;
    my $numProteins = 0;
    foreach my $id (keys %{$proteinIds}) {
	my $numDomains = scalar @{$proteinIds->{$id}->{domain}};
	my $numEcs = scalar @{$proteinIds->{$id}->{ec}};
	if ($numDomains > 0 && $numEcs > 0) {
	    $numProteins++;
	}
    }
    return $numProteins;
}

sub numProteinsWithEc {
    my ($proteinIds) = @_;
    my $numProteins = 0;
    foreach my $id (keys %{$proteinIds}) {
	my $numEcs = scalar @{$proteinIds->{$id}->{ec}};
	$numProteins++ if ($numEcs > 0);
    }
    return $numProteins;
}

sub getViableEcNumbers {
    my ($proteinIds,$test,$trainingIds,$minNumGenera,$minNumProteins,$statsFh) = @_;

    my $actualEcNumbers = &getUniqueEcNumbersFromProteins($proteinIds,$test,$trainingIds);
    my $allEcNumbers = &addPartialEcNumbers($actualEcNumbers);
    my $ecNumbersWithCounts = &getNumProteinsGeneraForEachEc($proteinIds,$test,$trainingIds,$allEcNumbers);
    &deletePartialEcNumbers($ecNumbersWithCounts);
    &deleteEcNumbersBelowMin($ecNumbersWithCounts,$minNumProteins,$minNumGenera);
    &printViableEcNumbers($ecNumbersWithCounts,$statsFh) if ($statsFh);
    
    return $ecNumbersWithCounts;
}

sub printViableEcNumbers {
    my ($ecNumbersWithCounts,$statsFh) = @_;
    print $statsFh "EC_NUMBER\tNUM_PROTEINS\tNUM_GENERA\n";
    foreach my $ec (sort keys %{$ecNumbersWithCounts}) {
	print $statsFh "$ec\t$ecNumbersWithCounts->{$ec}->{numProteins}\t$ecNumbersWithCounts->{$ec}->{numGenera}\n";
    }
    print $statsFh "\n";
}

sub getUniqueEcNumbersFromProteins {
    my ($proteinIds,$test,$trainingIds) = @_;
    my $ecs;
    foreach my $id (keys %{$proteinIds}) {
	next if ($test && ! exists $trainingIds->{$id});
	foreach my $ec ( @{$proteinIds->{$id}->{ec}} ) {
	    $ecs->{$ec}++;
	}
    }
    return $ecs;
}

sub validEcNumber {
    my ($ec) = @_;
    if ( $ec =~ /^[0-9]+\.[0-9]+\.[0-9\-]+\.[0-9\-]+$/ ) {
	return 1;
    } else {
	return 0;
    }
}

sub addPartialEcNumbers {
    my ($ecs) = @_;
    my $allEcs;
    foreach my $ec (keys %{$ecs}) {
	$allEcs->{$ec} = 1;
	my ($a,$b,$c,$d) = split(/\./,$ec);
	$allEcs->{"$a.$b.$c.-"} = 1;
	$allEcs->{"$a.$b.-.-"} = 1;
    }
    return $allEcs;
}

sub getNumProteinsGeneraForEachEc {
    my ($proteinIds,$test,$trainingIds,$allEcNumbers) = @_;
    my $ecNumbersWithCounts;
    my $ecGenera;
    foreach my $id (keys %{$proteinIds}) {
	next if ($test && ! exists $trainingIds->{$id});
	my $genus = &getGenusFromProtein($proteinIds->{$id});
	foreach my $ec ( keys %{$allEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$id},$ec)) {
		$ecNumbersWithCounts->{$ec}->{numProteins}++;
		$ecGenera->{$ec} = &addArrayElement($ecGenera->{$ec},$genus);
	    }
	}
    }
    
    &addNumberGenera($ecNumbersWithCounts,$ecGenera);
    return $ecNumbersWithCounts;
}

sub getGenusFromProtein {
    my ($protein) = @_;
    my @taxonArray = split(" ",$protein->{taxon});
    return $taxonArray[0];
}

sub addArrayElement {
    my ($arrayRef,$element) = @_;
    if (defined $arrayRef) {
	push @{$arrayRef}, $element;
    } else {
	$arrayRef = [$element];
    }
    return $arrayRef;
}

sub addNumberGenera {
    my ($ecNumbersWithCounts,$ecGenera) = @_;
    foreach my $ec (keys %{$ecNumbersWithCounts}) {
	my %allGenera = map { $_ => 1 } @{$ecGenera->{$ec}};
	my $numGenera = scalar keys %allGenera;
	$ecNumbersWithCounts->{$ec}->{numGenera} = $numGenera
    }
}

sub deleteEcNumbersBelowMin {
    my ($ecs,$minNumProteins,$minNumGenera) = @_;
    my %toDelete;
    foreach my $ec (keys %{$ecs}) {
	if ($ecs->{$ec}->{numProteins} < $minNumProteins || $ecs->{$ec}->{numGenera} < $minNumGenera) {
	    $toDelete{$ec} = 1;
	}
    }
    foreach my $ec (keys %toDelete) {
	delete $ecs->{$ec};
    }
}

sub deletePartialEcNumbers {
    my ($ecs) = @_;
    my %toDelete;
    foreach my $ec (keys %{$ecs}) {
	my $parentEc = getParent($ec);
	if ($parentEc && exists $ecs->{$parentEc}) {
	    if ($ecs->{$ec}->{numProteins} == $ecs->{$parentEc}->{numProteins}) {
		$toDelete{$parentEc} = 1;      # if parent has same number proteins then delete parent
	    }
	}
    }
    foreach my $ec (keys %toDelete) {
	delete $ecs->{$ec};
    }
}

sub getParent {
    my ($ec) = @_;
    return "" if ($ec eq "");
    my ($a,$b,$c,$d) = split(/\./,$ec);
    return "" if ($c eq "-");
    return "$a.$b.-.-" if ($d eq "-");
    return "$a.$b.$c.-";
}

sub getBackgroundDomainCount {
    my ($domainCountFile,$dbh,$excludeOld) = @_;
    my $domainCount;    
    if (-e $domainCountFile) {
	$domainCount = &readDomainCountFile($domainCountFile);
    } else {
	$domainCount = &countAllDomainsFromDatabase($dbh,$excludeOld);
	&writeDomainCountFile($domainCount,$domainCountFile);
	$domainCount->{numProteins} = &getNumProteinsFromDatabase($dbh,$excludeOld);
    }
    return $domainCount;
}

sub readDomainCountFile {
    my ($domainCountFile) = @_;
    my $domainCount;

    open(IN,$domainCountFile) || die "Cannot open $domainCountFile\n";
    my $header = <IN>;
    my $totalLine = <IN>;
    chomp($totalLine);
    my ($text,$numProteins) = split("\t",$totalLine);
    $domainCount->{numProteins} = $numProteins;
    while (my $line=<IN>) {
	chomp($line);
	my ($domain,$num) = split("\t",$line);
	$domainCount->{domain}->{$domain} = $num;
    }
    close IN;
    return $domainCount;
}

sub getNumProteinsFromDatabase {
    my ($dbh,$excludeOld) = @_;
    my $numProteins=0;

    my $query = $dbh->prepare(&numProteinsSql($excludeOld));
    $query->execute();
    while (my($count) = $query->fetchrow_array()) {
	$numProteins = $count;
    }
    $query->finish();
    return $numProteins;
}


sub ecLengthStats {
    my ($proteinIds,$test,$trainingIds,$viableEcNumbers,$statsFh) = @_;

    # for each viable EC number, obtain array of protein lengths
    my $ecNumbers;
    foreach my $id (keys %{$proteinIds}) {
	next if ( scalar @{$proteinIds->{$id}->{ec}} == 0 );
	next if ($test && ! exists $trainingIds->{$id});
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$id},$viableEc)) {
		$ecNumbers->{$viableEc} = &addArrayElement($ecNumbers->{$viableEc},$proteinIds->{$id}->{length});
	    }
	}
    }

    # for each EC number, calculate min, max, avg, median
    my $ecStats;
    foreach my $ec (keys %{$ecNumbers}) {
	my $noValues = scalar @{$ecNumbers->{$ec}} == 0 ? 1 : 0;
	$ecStats->{$ec}->{numProteins} = scalar @{$ecNumbers->{$ec}};
	$ecStats->{$ec}->{min} = $noValues ? -1 : min($ecNumbers->{$ec});
	$ecStats->{$ec}->{max} = $noValues ? -1 : max($ecNumbers->{$ec});
	$ecStats->{$ec}->{median} = $noValues ? -1 : median($ecNumbers->{$ec});
	my ($mean,$sd) = meanSd($ecNumbers->{$ec});
	$ecStats->{$ec}->{mean} = $noValues ? -1 : $mean;
	$ecStats->{$ec}->{sd} = $noValues ? -1 : $sd;
    }

    &printEcLengthStats($ecStats,$statsFh) if ($statsFh);

    return $ecStats;
}

sub printEcLengthStats {
    my ($ecStats,$statsFh) = @_;
    print $statsFh "EC_NUMBER\tNUM_PROTEINS\tMIN_LENGTH\tMAX_LENGTH\t_MEDIAN_LENGTH\tMEAN_LENGTH\tSTD_DEV_LENGTH\n";
    foreach my $ec (sort keys %{$ecStats}) {
	    print $statsFh "$ec\t$ecStats->{$ec}->{numProteins}";
	    print $statsFh "\t$ecStats->{$ec}->{min}\t$ecStats->{$ec}->{max}";
	    print $statsFh "\t$ecStats->{$ec}->{median}\t$ecStats->{$ec}->{mean}";
	    print $statsFh "\t$ecStats->{$ec}->{sd}\n";
    }
    print $statsFh "\n";
}

sub min {
    my ($arrayRef) = @_;
    my $min = $arrayRef->[0];
    foreach my $number (@{$arrayRef}) {
	$min = $number if ($number < $min);
    }
    return $min;
}

sub max {
    my ($arrayRef) = @_;
    my $max = $arrayRef->[0];
    foreach my $number (@{$arrayRef}) {
	$max = $number if ($number > $max);
    }
    return $max;
}

sub meanSd {
    my ($arrayRef) = @_;
    my $numSamples = scalar @{$arrayRef};
    my $sum = &sum($arrayRef);
    my $mean = $sum/$numSamples;

    $sum=0;
    foreach my $number (@{$arrayRef}) {
	$sum += ($number-$mean)**2;
    }
    my $sd = sqrt($sum/$numSamples);

    return ($mean,$sd);
}

sub sum {
    my ($arrayRef) = @_;
    my $sum = 0;
    foreach my $number (@{$arrayRef}) {
        $sum += $number;
    }
    return $sum;
}

sub median {
    my ($arrayRef) = @_;
    my @sorted = sort {$a <=> $b} @{$arrayRef};
    my $length = scalar @{$arrayRef};    
    if ( $length%2 ) {
	return $sorted[int($length/2)];
    } else {
        return ($sorted[int($length/2)-1] + $sorted[int($length/2)])/2;
    }
}

sub proteinHasThisEcNumber {
    my ($protein,$ecNumber) = @_;
    my ($a1,$b1,$c1,$d1) = split(/\./,$ecNumber);
    foreach my $currentEc ( @{$protein->{ec}} ) {
	my ($a2,$b2,$c2,$d2) = split(/\./,$currentEc);
	$c2 = "-" if ($c1 eq "-");
	$d2 = "-" if ($d1 eq "-");
	if (($a1 eq $a2) && ($b1 eq $b2) && ($c1 eq $c2) && ($d1 eq $d2)) {
	    return 1;
	}
    }
    return 0;
}

sub ecDomainStats {
    my ($proteinIds,$test,$trainingIds,$viableEcNumbers,$statsFh) = @_;

    my $domainToLetter = &getDomainKey($proteinIds,$statsFh);
       
    my ($ecNumbers,$domainPerProtein) = &getAllDomainStringsPerEc($proteinIds,$test,$trainingIds,$viableEcNumbers,$domainToLetter);
    &calculateDomainNumAndScore($ecNumbers,$viableEcNumbers);
    &calculateDomainMaxScore($ecNumbers);

    &printEcDomainStats($ecNumbers,$statsFh) if ($statsFh);
    &printProteinDomains($domainPerProtein,$statsFh) if ($statsFh);

    return ($ecNumbers,$domainPerProtein);
}

sub printProteinDomains {
    my ($domainPerProtein,$statsFh) = @_;
    print $statsFh "GENE\tDOMAINS\n";
    foreach my $id (keys %{$domainPerProtein}) {
	my $domainString = $domainPerProtein->{$id};
	print $statsFh "$id\t$domainString\n";
    }
    print $statsFh "\n";
}

sub printEcDomainStats {
    my ($ecNumbers,$statsFh) = @_;
    print $statsFh "EC_NUMBER\tDOMAIN_STRING\tNUM_PROTEINS\tSCORE\n";
    foreach my $ec (sort keys %{$ecNumbers}) {
	print $statsFh "$ec\t--NUM PROTEINS--\t$ecNumbers->{$ec}->{numProteins}\t$ecNumbers->{$ec}->{maxScore}\n";
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    print $statsFh "$ec\t$string";
	    print $statsFh "\t$ecNumbers->{$ec}->{domainString}->{$string}->{count}";
	    print $statsFh "\t$ecNumbers->{$ec}->{domainString}->{$string}->{score}\n";
	}
    }
    print $statsFh "\n";
}

sub getAllDomainStringsPerEc {
    my ($proteinIds,$test,$trainingIds,$viableEcNumbers,$domainToLetter) = @_;
    my ($ecNumbers,$domainPerProtein);

    foreach my $id (keys %{$proteinIds}) {
	my $domainString = &getDomain($proteinIds->{$id},$domainToLetter);
	$domainPerProtein->{$id} = $domainString;
	next if (scalar @{$proteinIds->{$id}->{ec}} == 0);
	next if ($test && ! exists $trainingIds->{$id});
	my $domains = &getAllPossibleCombinations($domainString,"");
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$id},$viableEc)) {
		foreach my $domain (keys %{$domains}) {
		    $ecNumbers->{$viableEc}->{domainString}->{$domain}->{count}++;
		}
	    }
	}
    }
    return ($ecNumbers,$domainPerProtein);
}

sub calculateDomainNumAndScore {
    my ($ecNumbers,$viableEcNumbers) = @_;
    foreach my $ec (keys %{$ecNumbers}) {
	$ecNumbers->{$ec}->{numProteins} = $viableEcNumbers->{$ec}->{numProteins};
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    $ecNumbers->{$ec}->{domainString}->{$string}->{score} =  $ecNumbers->{$ec}->{domainString}->{$string}->{count} / $viableEcNumbers->{$ec}->{numProteins};
	}
    }
}

sub calculateDomainMaxScore {
    my ($ecNumbers) = @_;
    foreach my $ec (keys %{$ecNumbers}) {
	$ecNumbers->{$ec}->{maxScore} = 0;
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    $ecNumbers->{$ec}->{maxScore} += $ecNumbers->{$ec}->{domainString}->{$string}->{score};
	}
    }
}

sub getDomain {
    my ($proteinRef,$domainToLetter) = @_;
    my $domain = "-";
    if (scalar @{$proteinRef->{domain}} > 0) {
	my @domainLetters = map { $domainToLetter->{$_} } @{$proteinRef->{domain}};
	$domain = join("",@domainLetters);
    }
    return $domain;
}

sub numProteinScore {
    my ($numProteinsWithEc) = @_;
    if ($numProteinsWithEc > 1) {
	return 1;
    } else {
	return 0;
    }
}

sub numGeneraScore {
    my ($numGeneraWithEc) = @_;
    if ($numGeneraWithEc > 1) {
	return 1;
    } else {
	return 0;
    }
}

sub domainScore {
    my ($id,$ec,$domainStatsPerEc,$domainPerProtein) = @_;
    my $score=0;
    my $idDomain = $domainPerProtein->{$id};
    return -1 if (! $idDomain || ! exists $domainStatsPerEc->{$ec});
    foreach my $domainString ( keys %{$domainStatsPerEc->{$ec}->{domainString}} ) {
	if ($idDomain =~ /$domainString/) {
	    $score += $domainStatsPerEc->{$ec}->{domainString}->{$domainString}->{score};
	}
    }
    my $normalizedScore = $score / $domainStatsPerEc->{$ec}->{maxScore};
    if ($normalizedScore > 0.8 ) {
	return 4;
    } elsif ($normalizedScore > 0.6) {
	return 3;
    } elsif ($normalizedScore > 0.4) {
	return 2;
    } elsif ($normalizedScore > 0.2) {
	return 1;
    } else {
	return 0;
    }
}

sub lengthScore {
    my ($id,$ec,$proteinIds,$lengthStatsPerEc) = @_;

    my $idLength = $proteinIds->{$id}->{length};
    my $idDistanceFromMedian = abs($idLength - $lengthStatsPerEc->{$ec}->{median});
    my $tenPercentOfMedian = 0.1 * $lengthStatsPerEc->{$ec}->{median};
    
    if ($idDistanceFromMedian <= $tenPercentOfMedian ) {
	return 4;
    } elsif ($idDistanceFromMedian <= 2*$tenPercentOfMedian) {
	return 3;
    } elsif ($idDistanceFromMedian <= 3*$tenPercentOfMedian) {
	return 2;
    } elsif ($idLength >= $lengthStatsPerEc->{$ec}->{min} && $idLength <= $lengthStatsPerEc->{$ec}->{max}) {
	return 1;
    } else {
	return 0;
    }
}

sub blastScore {
    my ($id,$ec,$blastStatsPerEc,$blastStatsPerProtein) = @_;

    return 0 if (! exists $blastStatsPerProtein->{$id}->{$ec});   #this protein does not BLAST to any protein with an EC number

    my $idBlast = $blastStatsPerProtein->{$id}->{$ec}->{median};
    my $ecBlast =  $blastStatsPerEc->{$ec}->{median};
    $ecBlast = -181 if ($ecBlast == 0);  # this happens when protein with EC does not have blast partners
    my $tenPercentOfMedian = abs(0.1 * $ecBlast);

    if ($idBlast <= ($ecBlast+$tenPercentOfMedian) ) {
	return 4;
    } elsif ($idBlast <= ($ecBlast+2*$tenPercentOfMedian) ) {
	return 3;
   } elsif ($idBlast <= ($ecBlast+3*$tenPercentOfMedian) ) {
	return 2;
    } elsif ($idBlast <= $blastStatsPerEc->{$ec}->{max} ) {
	return 1;
    } else {
	return 0;
    }
}

sub getProteinScores {
    my ($proteinIds,$numTotalProteins,$numTotalGenera,$test,$testIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$organisms,$fHs) = @_;

    my $scores;
    foreach my $id (keys %{$proteinIds}) {
	next if ($test && ! exists $testIds->{$id});
	foreach my $ec (keys %{$viableEcNumbers}) {
	    $scores->{$id}->{$ec}->{numProteinsWithEc} = $viableEcNumbers->{$ec}->{numProteins};
	    $scores->{$id}->{$ec}->{numGeneraWithEc} = $viableEcNumbers->{$ec}->{numGenera};
	    $scores->{$id}->{$ec}->{lengthScore} = &lengthScore($id,$ec,$proteinIds,$lengthStatsPerEc);
	    $scores->{$id}->{$ec}->{blastScore} = &blastScore($id,$ec,$blastStatsPerEc,$blastStatsPerProtein);
	    $scores->{$id}->{$ec}->{domainScore} = &domainScore($id,$ec,$domainStatsPerEc,$domainPerProtein);
	    $scores->{$id}->{$ec}->{numTotalProteins} = $numTotalProteins;
	    $scores->{$id}->{$ec}->{numTotalGenera} = $numTotalGenera;
	}
    }
    &deletePartialEcWithWorseScore($scores);
    &printEcScores($scores,$organisms,$fHs) if (! $test);
    return $scores;
}

sub deletePartialEcWithWorseScore {
    my ($scores) = @_;
    foreach my $id (keys %{$scores}) {
	my %toDelete;
	foreach my $ec (keys %{$scores->{$id}}) {
	    my $parentEc = &getParent($ec);
	    if ($parentEc && exists $scores->{$id}->{$parentEc}) {
		my $score = &scoreToNumber($scores->{$id}->{$ec});
		my $parentScore = &scoreToNumber($scores->{$id}->{$parentEc});
		if ($score >= $parentScore) {
		    $toDelete{$parentEc} = 1;      # if parent has same or worse score, then delete parent
		}
	    }
	}
	foreach my $ec (keys %toDelete) {
	    delete $scores->{$id}->{$ec};
	}
    }
}

sub scoreToNumber {
    my ($scoreRef) = @_;
    my $score = $scoreRef->{domainScore} == -1 ? 0 : $scoreRef->{domainScore}/10;
    $score += $scoreRef->{lengthScore} + $scoreRef->{blastScore};
    return $score;
}

sub printEcScores {
    my ($scores,$organisms,$fHs) = @_;
    foreach my $id (keys %{$scores}) {
	my $editedId = $id;
	my $abbrev;
	if ($id =~ /^([a-z]{4})\|/) {
	    $abbrev = $1;
	} else {
	    die "Could not find organism abbrev for this id: '$id'"
	}
	if ($abbrev eq "rhiz") { # this is temporary, because rhiz on orthomcl equals rirr on fungidb
	    $abbrev = "rirr";
	    $editedId =~ s/^rhiz/rirr/;
	}
	my @fhsToPrint;
	next if (exists $organisms->{$abbrev} && $organisms->{$abbrev} eq "none");
	if (! exists $organisms->{$abbrev} || ! exists $fHs->{$organisms->{$abbrev}}) {
	    @fhsToPrint = map { $fHs->{$_} } keys %{$fHs};
	} else {
	    push @fhsToPrint, $fHs->{$organisms->{$abbrev}};
	}
	foreach my $ec (keys %{$scores->{$id}}) {
	    my $string = &scoresToString($scores->{$id}->{$ec});
	    my $text = "$editedId\t$ec\t$string\n";
	    print $_ $text foreach (@fhsToPrint);
	}
    }
}

sub scoresToString {
    my ($scoresRef) = @_;
    my $a = $scoresRef->{numProteinsWithEc};
    my $b = $scoresRef->{numTotalProteins};
    my $c = $scoresRef->{numGeneraWithEc};
    my $d = $scoresRef->{numTotalGenera};
    my $e = $scoresRef->{lengthScore};
    my $f = $scoresRef->{blastScore};
    my $g = $scoresRef->{domainScore};
    return "$a\t$b\t$c\t$d\t$e\t$f\t$g";
}

sub testScores {
    my ($scores,$proteinIds,$testIds,$totalEcsRef,$noEcMatchRef,$testExact,$testLessPrecise,$testMorePrecise,$numProteinsExtraEcRef,$numExtraEcRef,$numTotalProteinsRef,$testFh,$logFh) = @_;
    my $group = &getGroupFromProteins($proteinIds,$logFh);
    print $testFh "Group $group\n";
    foreach my $id (keys %{$testIds}) {
	foreach my $thisIdEc (@{$proteinIds->{$id}->{ec}}) {
	    ${$totalEcsRef}++;
	    print $testFh "$id\t$thisIdEc\t";
	    &testEcNumberMatch($thisIdEc,$scores->{$id},$noEcMatchRef,$testExact,$testLessPrecise,$testMorePrecise,$testFh);
	}
	&testExtraEc($id,$proteinIds->{$id}->{ec},$scores->{$id},$numProteinsExtraEcRef,$numExtraEcRef,$testFh);
	${$numTotalProteinsRef}++;
    }
}

sub testExtraEc {
    my ($id,$idEcs,$scoresForThisId,$numProteinsExtraEcRef,$numExtraEcRef,$testFh) = @_;
    my $proteinNoMatch = 0;
    foreach my $predictedEc (keys %{$scoresForThisId}) {
	my $ecMatch = 0;
	foreach my $idEc (@{$idEcs}) {
	    if ($idEc eq $predictedEc) {
		$ecMatch = 1;
		last;
	    }
	}
	if ($ecMatch == 0) {
	    $proteinNoMatch = 1;
	    my $string = &scoresToString($scoresForThisId->{$predictedEc});
	    print $testFh "$id\t$predictedEc\t$string\tnew_assignment\n";
	    ${$numExtraEcRef}++;
	}
    }
    ${$numProteinsExtraEcRef}++ if ($proteinNoMatch == 1);
}

sub testEcNumberMatch {
    my ($thisIdEc,$scoresForThisId,$noEcMatchRef,$testExact,$testLessPrecise,$testMorePrecise,$testFh) = @_;
    if (exists $scoresForThisId->{$thisIdEc}) {   #exact match
	my $string = &scoresToString($scoresForThisId->{$thisIdEc});
	my $score = &scoreToNumber($scoresForThisId->{$thisIdEc});
	$testExact->{$score}++;
	print $testFh "$string\texact\n";
	return;
    }
    if (&testMorePrecise($thisIdEc,$scoresForThisId,$testMorePrecise,$testFh)) {
	return;
    }
    if (&testLessPrecise($thisIdEc,$scoresForThisId,$testLessPrecise,$testFh)) {
	return;
    }
    &testNoMatch($noEcMatchRef,$scoresForThisId,$testFh);
}

sub testNoMatch {
    my ($noEcMatchRef,$scoresForThisId,$testFh) = @_;
    ${$noEcMatchRef}++;
    print $testFh "\t\t\t\t\t\t\tno_match,ECs_predicted: ";
    foreach my $thisIdEc (sort keys %{$scoresForThisId}) {
	my $score = &scoreToNumber($scoresForThisId->{$thisIdEc});
	print $testFh "$thisIdEc($score)";
    }
    print $testFh "\n";
}

sub testLessPrecise {
    my ($thisIdEc,$scoresForThisId,$testLessPrecise,$testFh) = @_;
    my $thisIdParentEc = &getParent($thisIdEc);
    if (! $thisIdParentEc) {
	return 0;
    } elsif (exists $scoresForThisId->{$thisIdParentEc}) {
	my $string = &scoresToString($scoresForThisId->{$thisIdParentEc});
	my $score = &scoreToNumber($scoresForThisId->{$thisIdParentEc});	
	print $testFh "$string\tprediction_less_precise: $thisIdParentEc\n";
	$testLessPrecise->{$score}++;
	return 1;
    } else {
	return &testLessPrecise($thisIdParentEc,$scoresForThisId,$testLessPrecise,$testFh);
    }
}

sub testMorePrecise {
    my ($thisIdEc,$scoresForThisId,$testMorePrecise,$testFh) = @_;
    foreach my $predictedEc (keys %{$scoresForThisId}) {
	if (&partialMatchEc($thisIdEc,$predictedEc)) {
	    my $string = &scoresToString($scoresForThisId->{$predictedEc});
	    my $score = &scoreToNumber($scoresForThisId->{$predictedEc});	
	    print $testFh "$string\tprediction_more_precise: $predictedEc\n";
	    $testMorePrecise->{$score}++;
	    return 1;
	}
    }
    return 0;
}

sub partialMatchEc {
    my ($thisIdEc,$predictedEc) = @_;
    my $predictedParentEc = &getParent($predictedEc);
    if (! $predictedParentEc) {
	return 0;
    } elsif ($thisIdEc eq $predictedParentEc) {
	return 1;
    } else {
	return &partialMatchEc($thisIdEc,$predictedParentEc);
    }
}
 
sub printTest {
    my ($total,$noMatch,$testExact,$testLessPrecise,$testMorePrecise,$numProteinsExtraEcRef,$numExtraEcRef,$numTotalProteinsRef,$testFh) = @_;
    my $percentNoMatch = sprintf("%.1f",100 * ${$noMatch} / ${$total});
    my $numExactMatch = &sumHashValues($testExact);
    my $percentExactMatch = sprintf("%.1f",100 * $numExactMatch / ${$total});
    my $numLessPreciseMatch = &sumHashValues($testLessPrecise);
    my $percentLessPreciseMatch = sprintf("%.1f",100 * $numLessPreciseMatch / ${$total});
    my $numMorePreciseMatch = &sumHashValues($testMorePrecise);
    my $percentMorePreciseMatch = sprintf("%.1f",100 * $numMorePreciseMatch / ${$total});
    my $percentProteinsExtraEc = sprintf("%.1f",100 * ${$numProteinsExtraEcRef} / ${$numTotalProteinsRef});
    print $testFh "\nSUMMARY\n";
    print $testFh "Total tested ECs\t${$total}\n";
    print $testFh "No match\t${$noMatch} ($percentNoMatch %)\n";
    print $testFh "Exact match\t$numExactMatch ($percentExactMatch %)\n";
    print $testFh "Less precise match\t$numLessPreciseMatch ($percentLessPreciseMatch %)\n";
    print $testFh "More precise match\t$numMorePreciseMatch ($percentMorePreciseMatch %)\n";
    print $testFh "Extra ECs\t${$numExtraEcRef}\n";
    print $testFh "\nTotal tested proteins\t${$numTotalProteinsRef}\n";
    print $testFh "Num proteins w/ extra predicted EC\t${$numProteinsExtraEcRef} ($percentProteinsExtraEc %)\n";
    print $testFh "\nExact predictions:\n";
    foreach my $score (sort { $b cmp $a } keys %{$testExact}) {
	print $testFh "$score\t$testExact->{$score}\n";
    }
    print $testFh "Less precise predictions:\n";
    foreach my $score (sort { $b cmp $a } keys %{$testLessPrecise}) {
	print $testFh "$score\t$testLessPrecise->{$score}\n";
    }
    print $testFh "More precise predictions:\n";
    foreach my $score (sort { $b cmp $a } keys %{$testMorePrecise}) {
	print $testFh "$score\t$testMorePrecise->{$score}\n";
    }

    close $testFh;
}

sub sumHashValues {
    my ($hashRef) = @_;
    my $sum = 0;
    foreach my $key (keys %{$hashRef}) {
	$sum += $hashRef->{$key};
    }
    return $sum;
}
    
sub getAllPossibleCombinations {
    my ($domainString,$delimiter) = @_;

    my $domains;
    my @domainArray = split(/$delimiter/,$domainString);
    my $length = scalar @domainArray;
    for (my $a=0; $a<$length; $a++) {
	for (my $b=$a; $b<$length; $b++) {
	    my $string = join("$delimiter",@domainArray[$a..$b]);
	    $domains->{$string} = 1;
	}
    }
    return $domains;
}


sub getDomainKey {
    my ($proteinIds,$statsFh) = @_;
    
    my $domains;

    foreach my $protein (keys %{$proteinIds}) {
	foreach my $domain (@{$proteinIds->{$protein}->{domain}}) {
	    $domains->{$domain}++;
	}
    }

    my @alphabet = ("A".."Z","a".."z",0..9);
    my $numCharacters = scalar @alphabet;
    my $numDomains = scalar keys %{$domains};
    # stringLength is how long string needs to be in order to capture all domains
    # 1 if <= 62 domains, 2 if <= 3844, 3 if <= 238328, 4 if <= 14776336  (max length is set to 4)
    my $stringLength = 0;
    for (my $a=1; $a<=4; $a++) {
	if ($numDomains <= $numCharacters**$a) {
	    $stringLength = $a;
	    last;
	}
    }
    die "There are more than 14,776,336 domains in this group so cannot perform A-Z a-z 0-9 mapping" if ($stringLength == 0);

    my $stringCounter = &initializeStringCounter($stringLength);
    my @domainArray = sort { $domains->{$b} <=> $domains->{$a} } keys %{$domains};
    
    print $statsFh "DOMAIN\tSTRING\n" if ($statsFh);
    for (my $currentDomain=0; $currentDomain<$numDomains; $currentDomain++) {
	my $currentString = &makeStringFromCounter($stringCounter,\@alphabet);
	$domains->{$domainArray[$currentDomain]} = $currentString;
	print $statsFh "$domainArray[$currentDomain]\t$currentString\n" if ($statsFh);
	$stringCounter = &increaseStringCounter($stringCounter,$numCharacters);
    }
    print $statsFh "\n" if ($statsFh);
    
    return $domains;
}

sub initializeStringCounter {
    my ($stringLength) = @_;
    my $stringCounter;
    for (my $a=0; $a<$stringLength; $a++) {
	push @{$stringCounter}, 0;
    }
    return $stringCounter;
}

sub makeStringFromCounter {
    my ($stringCounter,$alphabet) = @_;
    my @string = map { $alphabet->[$_] } @{$stringCounter};
    return join("",@string);
}

sub increaseStringCounter {
    my ($stringCounter,$numCharacters) = @_;

    my $numDigits = scalar @{$stringCounter};

    for (my $i=($numDigits-1); $i>=0; $i--) {
	if ( $stringCounter->[$i] == ($numCharacters-1) ) {
	    $stringCounter->[$i] = 0;
	} else {
	    $stringCounter->[$i]++;
	    last;
	}
    }
    return $stringCounter;
    
}

sub writeProteinInfoFile {
    my ($proteinIds,$numTotalProteins,$numTotalGenera,$proteinInfoFile) =@_;

    open(OUT,">",$proteinInfoFile) || die "Cannot open '$proteinInfoFile' for writing";
    print OUT "Total num proteins\t$numTotalProteins\n";
    print OUT "Total num genera\t$numTotalGenera\n";
    print OUT "GROUP\tPROTEIN_ID\tTAXON\tLENGTH\tDOMAIN\tPRODUCT\tEC_NUMBER\n";
    foreach my $id (keys %{$proteinIds}) {
	my $group = $proteinIds->{$id}->{group};
	my $length = $proteinIds->{$id}->{length};
	my $product = $proteinIds->{$id}->{product};
	my $taxon = $proteinIds->{$id}->{taxon};
	my @ecs = @{$proteinIds->{$id}->{ec}};
	$ecs[0] = "-" if (scalar @ecs == 0);
	my $ec = join(",",@ecs);
	my @domains = @{$proteinIds->{$id}->{domain}};
	$domains[0] = "-" if (scalar @domains == 0);
	my $domain = join(",",@domains);
	print OUT "$group\t$id\t$taxon\t$length\t$domain\t$product\t$ec\n";
    }
    close OUT; 
}

sub readProteinInfoFile {
    my ($proteinInfoFile,$excludeOld) = @_;
    my $proteinIds;
    open(IN,$proteinInfoFile) || die "Cannot open '$proteinInfoFile' for reading";
    my $numTotalProteins = &getSecondColumn(<IN>);
    my $numTotalGenera = &getSecondColumn(<IN>);
    while (my $line = <IN>) {
	chomp($line);
	next if ($line !~ /^OG/);
	my ($group,$id,$taxon,$length,$domains,$product,$ecs) = split("\t",$line);
	next if ($excludeOld && $id =~ /-old\|/);
	$proteinIds->{$id}->{group} = $group;
	$proteinIds->{$id}->{taxon} = $taxon;
	$proteinIds->{$id}->{length} = $length;
	$proteinIds->{$id}->{product} = $product;
	$ecs = $ecs eq "-" ? "" : $ecs;
	$proteinIds->{$id}->{ec} = [split(",",$ecs)];
	$domains = $domains eq "-" ? "" : $domains;
	$proteinIds->{$id}->{domain} = [split(",",$domains)];
    }
    close IN;
    return ($proteinIds,$numTotalProteins,$numTotalGenera);
}

sub getSecondColumn {
    my ($line) = @_;
    chomp $line;
    my @lineArray = split("\t",$line);
    return $lineArray[1];
}

sub getProteinInfo {
    my ($group,$excludeOld,$dbh,$proteinInfoFile) = @_;

    my $proteinIds;
    my $numTotalProteins;
    my $numTotalGenera;
    
    if ($proteinInfoFile && -e $proteinInfoFile) {
	($proteinIds,$numTotalProteins,$numTotalGenera) = &readProteinInfoFile($proteinInfoFile,$excludeOld);
    } else {
	($proteinIds,$numTotalProteins,$numTotalGenera) = &getProteinsFromDatabase($proteinIds,$group,$excludeOld,$dbh);
	$proteinIds = &getEcsFromDatabase($proteinIds,$group,$excludeOld,$dbh);
	$proteinIds = &getDomainsFromDatabase($proteinIds,$group,$excludeOld,$dbh);
	&writeProteinInfoFile($proteinIds,$numTotalProteins,$numTotalGenera,$proteinInfoFile) if ($proteinInfoFile);
    }
    return ($proteinIds,$numTotalProteins,$numTotalGenera);
}

sub getProteinsFromDatabase {
    my ($proteinIds,$group,$excludeOld,$dbh) = @_;
    my $numTotalProteins = 0;
    my %genera;
    my $query = $dbh->prepare(&proteinsSql($group));
    $query->execute();
    while (my($id,$product,$length,$corePeripheral,$group,$taxon) = $query->fetchrow_array()) {
	next if ( ($excludeOld && $id =~ /-old\|/) || $id eq "");
	$numTotalProteins++;
	$proteinIds->{$id}->{product} = $product;
	$proteinIds->{$id}->{length} = $length;
	$proteinIds->{$id}->{corePeripheral} = $corePeripheral;
	$proteinIds->{$id}->{group} = $group;
	$proteinIds->{$id}->{taxon} = $taxon;
	$proteinIds->{$id}->{ec} = [];
	$proteinIds->{$id}->{domain} = [];
	$genera{&getGenusFromProtein($proteinIds->{$id})} = 1;
    }    
    $query->finish();
    my $numTotalGenera = keys %genera;
    return ($proteinIds,$numTotalProteins,$numTotalGenera);
}

sub createIdString {
    my ($proteinIds,$excludeOld) = @_;
    my @ids;
    foreach my $id (keys %{$proteinIds}) {
	next if ($excludeOld && $id =~ /-old\|/);
	push @ids, $id;
    }
    my $idString = join("','",@ids);
    return "('".$idString."')";
}

sub getEcsFromDatabase {
    my ($proteinIds,$group,$excludeOld,$dbh) = @_;

    my $query = $dbh->prepare(&ecsSql($group));
    
    $query->execute();
    while (my($id,$ecString) = $query->fetchrow_array()) {
	next if ($excludeOld && $id =~ /-old\|/);
	die "The protein '$id' was not found in group" if (! exists $proteinIds->{$id});
	$ecString =~ s/ //g;
	my @multipleEc = split(/[;,]/,$ecString);
	foreach my $ec ( @multipleEc) {
	    next if (! &validEcNumber($ec));
	    push @{$proteinIds->{$id}->{ec}}, $ec;
	}
    }
    $query->finish();
    
    # sort EC numbers for each protein
    foreach my $id (keys %{$proteinIds}) {
	@{$proteinIds->{$id}->{ec}} = sort @{$proteinIds->{$id}->{ec}};
    }
    
    return $proteinIds;
}

sub getDomainsFromDatabase {
    my ($proteinIds,$group,$excludeOld,$dbh) = @_;

     my $query = $dbh->prepare(&domainsSql($group));

    $query->execute();
    while (my($id,$domain) = $query->fetchrow_array()) {
	next if ($excludeOld && $id =~ /-old\|/);
	die "The protein '$id' was not found in group" if (! exists $proteinIds->{$id});
	push @{$proteinIds->{$id}->{domain}}, $domain;
    }
    $query->finish();
    return $proteinIds;
}

sub countAllDomainsFromDatabase {
    my ($dbh,$excludeOld) = @_;
    my $query = $dbh->prepare(&allDomainsSql);
    $query->execute();
    my $domainCount;
    my %seen;
    while (my($id,$domain) = $query->fetchrow_array()) {
	next if ($excludeOld && $id =~ /-old\|/);
	next if ($seen{$id.$domain});  # because counting number proteins per domain
	$seen{$id.$domain} = 1;
	$domainCount->{domain}->{$domain}++;	
    }
    $query->finish();
    return $domainCount;
}

sub writeDomainCountFile {
    my ($domainCount,$domainCountFile) = @_;

    open(OUT,">",$domainCountFile) || die "Cannot open $domainCountFile for writing\n";
    print OUT "DOMAIN\tNUM_PROTEINS\n";
    print OUT "all_proteins\t$domainCount->{numProteins}\n";
    foreach my $domain (keys %{$domainCount->{domain}}) {
	print OUT "$domain\t$domainCount->{domain}->{$domain}\n";
    }
    close OUT;
}
   
sub getGroupFromProteins {
    my ($proteinIds,$logFh) = @_;
    my $group = "";
    foreach my $protein (keys %{$proteinIds}) {
	if ($group ne "" && $group ne $proteinIds->{$protein}->{group}) {
	    die "expected only one ortholog group but obtained more than one: $group $proteinIds->{$protein}->{group}";
	}
	$group = $proteinIds->{$protein}->{group};
    }
    if ($group eq "") {
	print {$logFh ? $logFh : *STDERR } "Did not obtain any ortholog groups from these proteins: ";
	print {$logFh ? $logFh : *STDERR } "$_ " foreach (keys %{$proteinIds});
	die;
    }
    return $group;
}

sub getGroupsFromDatabase {
    my ($minNumProteinsWithEc,$maxNumProteinsWithEc,$dbh) = @_;
    my %groups;
    
    my $query = $dbh->prepare(&groupsSql($minNumProteinsWithEc,$maxNumProteinsWithEc));

    $query->execute();
    while (my($group) = $query->fetchrow_array()) {
	
        $groups{$group} = 1;
    }
    $query->finish();
    return \%groups;
}

sub allDomainsSql {
    return "SELECT full_id,accession FROM ApidbTuning.DomainAssignment";
}

sub domainsSql {
    my ($group) = @_;
    return "SELECT full_id,accession FROM ApidbTuning.DomainAssignment
            WHERE group_name = '$group' ORDER BY full_id,start_min";
}

sub ecsSql {
    my ($group) = @_;
    return "SELECT sa.full_id,ec.ec_number
            FROM SRes.EnzymeClass ec, DoTS.AASequenceEnzymeClass aaec,
                 apidbTuning.SequenceAttributes sa
            WHERE ec.enzyme_class_id = aaec.enzyme_class_id AND aaec.aa_sequence_id = sa.aa_sequence_id
                  AND evidence_code NOT LIKE 'OrthoMCL%' AND sa.group_name='$group'";
}

sub proteinsSql {
    my ($group) = @_;
    return "SELECT full_id,product,length,core_peripheral,group_name,organism_name                                     
            FROM ApidbTuning.SequenceAttributes WHERE group_name='$group'";
}

sub groupsSql {
    my ($minNumProteinsWithEc,$maxNumProteinsWithEc) = @_;
    my $maxClause = "";
    if ($maxNumProteinsWithEc) {
	$maxClause = "AND num_proteins <= $maxNumProteinsWithEc";
    }
    return "SELECT group_name
            FROM (SELECT group_name,count(ec_numbers) as num_proteins                         
                  FROM ApidbTuning.SequenceAttributes sa
                  WHERE ec_numbers IS NOT NULL AND group_name IS NOT NULL
                  GROUP BY group_name)
            WHERE num_proteins >= $minNumProteinsWithEc $maxClause";
}

sub blastSql {
    my ($group) = @_;
    return "SELECT ssg.query_id, ssg.subject_id, ssg.evalue_mant, ssg.evalue_exp
            FROM apidb.similarSequencesGroupCore ssg, apidb.orthologGroup og
            WHERE ssg.ortholog_group_id=og.ortholog_group_id AND og.name='$group'";
}

sub orthoOrganismSql {
    my ($excludeOld) = @_;
    my $whereClause = $excludeOld ? "AND o.three_letter_abbrev NOT LIKE '%-old'" : "";
    return "SELECT o.three_letter_abbrev, p.three_letter_abbrev
            FROM apidb.OrthomclTaxon o,
                 (SELECT orthomcl_taxon_id,three_letter_abbrev
                  FROM apidb.OrthomclTaxon
                  WHERE core_peripheral='Z') p
            WHERE o.parent_id = p.orthomcl_taxon_id
                  AND o.core_peripheral IN ('C','P') $whereClause";
}

sub numProteinsSql {
    my ($excludeOld) = @_;
    my $whereClause = $excludeOld ? "WHERE secondary_identifier NOT LIKE '%-old|%'" : "";
    return "SELECT COUNT(*) FROM dots.ExternalAaSequence $whereClause";
}

sub getDbHandle {

  my $gusConfigFile = $ENV{GUS_HOME} . "/config/gus.config";
  my @properties = ();
  my $gusconfig = CBIL::Util::PropertySet->new($gusConfigFile, \@properties, 1);

  my $u = $gusconfig->{props}->{databaseLogin};
  my $pw = $gusconfig->{props}->{databasePassword};
  my $dsn = $gusconfig->{props}->{dbiDsn};

  my $dbh = DBI->connect($dsn, $u, $pw) or die DBI::errstr;
  $dbh->{RaiseError} = 1;
  $dbh->{AutoCommit} = 0;

  return $dbh;
}

sub makeDir {
    my ($dir) = @_;
    system("mkdir -p $dir") unless (-e $dir);
}

sub getCladeToProject {
    my %cladeToProject = (
        ALVE => "CryptoDB",
        APIC => "CryptoDB",
        HAEM => "PlasmoDB",
        PIRO => "PiroplasmaDB",
        AMOE => "AmoebaDB",
        EUGL => "TriTrypDB",
        FUNG => "FungiDB",
        MICR => "MicrosporidiaDB",
        BASI => "FungiDB",
        ASCO => "FungiDB",
        MUCO => "FungiDB",
        CHYT => "FungiDB",
	ARTH => "VectorBase",
	MAMM => "HostDB",
        OOMY => "FungiDB",
	ACTI => "none",
	ARCH => "none",
	VIRI => "none",
	CHOR => "none",
	CHLO => "none",
	CILI => "none",
	CREN => "none",
	CRYP => "none",
	EURY => "none",
	FIRM => "none",
	KORA => "none",
	FIRM => "none",
	NEMA => "none",
	OBAC => "none",
	PROG => "none",
	PROA => "none",
	PROB => "none",
	PROD => "none",
	PROE => "none",
	RHOD => "none",
	STRE => "none",
	TUNI => "none"
        );
    return \%cladeToProject;
}
