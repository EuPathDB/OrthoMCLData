#!/usr/bin/perl

use warnings;
use strict;
use Data::Dumper;
use lib "$ENV{GUS_HOME}/lib/perl";
use CBIL::Util::PropertySet;
use DBI;

# example command line:  orthomclEcPrediction.pl /home/markhick/EC
# good groups to study: OG6_100435, OG6_101725

my $outputDirectory = $ARGV[0];

# 1. get groups with 1+ EC numbers (10+ for testing)
# 2. loop through groups
# 3. get all protein info
# 3.b. if testing, make hash of proteins (with EC) to hide
# 4. get domain stats if there are domains
# 5. get length and blast stats
# 6. assign EC numbers, output file with EC and score

my $minNumProteinsWithEc = 500;
my $minNumGenera = 1;
my $minNumProteins = 1;
my $includeOld = 1;
my $createStatsFile = 1;
my $createProteinFile = 1;
my $test = 1;
my $testFraction = 0.2;


my $dbh = getDbHandle();

#my $groups = getGroupsFromDatabase($minNumProteinsWithEc,$dbh);

my $groups;
$groups->{"OG6_100718"} = 1;

foreach my $group (keys %{$groups}) {
    my ($statsFh,$proteinFile,$scoreFile) = &makeDirAndFiles($outputDirectory,$group,$createStatsFile,$createProteinFile);
    my $proteinIds = &getProteinInfo($group,$includeOld,$dbh,$proteinFile);

    my $testIds;
    if ($test) {
	($proteinIds,$testIds) = &getTrainingAndTest($proteinIds);
    }

    my $viableEcNumbers = &getViableEcNumbers($proteinIds,$minNumGenera,$minNumProteins,$statsFh);
    
    my $domainStatsPerEc;
    my $domainPerProtein;
    if (&numProteinsWithDomainAndEc($proteinIds) > 0) {
	($domainStatsPerEc,$domainPerProtein) = &ecDomainStats($proteinIds,$viableEcNumbers,$statsFh);
    }

    my $lengthStatsPerEc = &ecLengthStats($proteinIds,$viableEcNumbers,$statsFh);

    my ($blastStatsPerEc,$blastStatsPerProtein) = &ecBlastStats($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$statsFh);

    close $statsFh;

   my $scores =  &getProteinScores($proteinIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$groupScoresFile);

    if ($test) {
	&testScores($scores,$proteinIds,$testIds);
    }

}

$dbh->disconnect();
exit;




################################  SUBROUTINES  ########################################


sub makeDirAndFiles {
    my ($outputDirectory,$group,$createStatsFile,$createProteinFile) = @_;
    &makeDir("$outputDirectory/$group");
    my $statsFh;
    if ($createStatsFile) {
	my $statsFile = "$outputDirectory/$group/stats.txt";
	open($statsFh,">",$statsFile) || die "Cannot open $statsFile for writing\n";
    }
    my $proteinFile;
    if ($createProteinFile) {
	$proteinFile = "$outputDirectory/$group/proteins.txt";
    }
    my $scoresFile = "$outputDirectory/$group/scores.txt";
    return ($statsFh,$proteinFile,$scoreFile);
}

sub ecBlastStats {
    my ($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$statsFh) = @_;

    my ($blastEvalues,$blastPerEc) = &readBlastEvaluesFromDatabase($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$statsFh);
    my @orderedViableEcs = sort keys %{$viableEcNumbers};
    my $blastStatsPerEc = &calculateBlastStatsPerEc(\@orderedViableEcs,$blastPerEc,$statsFh);
    my $blastStatsPerProtein = &calculateBlastStatsPerProtein(\@orderedViableEcs,$blastEvalues,$statsFh);
    
    return ($blastStatsPerEc,$blastStatsPerProtein);
}

sub readBlastEvaluesFromDatabase {
    my ($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$statsFh) = @_;

    my $blastEvalues;    # id -> ec -> (-5,-6.4,-150.3)
    my $blastPerEc;        # ec -> (-5,-6.4,-150.3)  only if both proteins have EC
    my $missing;
    my $query = $dbh->prepare(&blastSql($group));
    
    $query->execute();
    while (my($query,$subject,$mantua,$exponent) = $query->fetchrow_array()) {
	next if ($includeOld && ($query =~ /-old\|/ || $subject =~ /-old\|/));
	next if (&proteinDoesNotExist($subject,$proteinIds,$statsFh,$missing));
	next if (&proteinDoesNotExist($query,$proteinIds,$statsFh,$missing));
	
	my $subjectHasEc =  scalar @{$proteinIds->{$subject}->{ec}} > 0 ? 1 : 0;
	my $queryHasEc =  scalar @{$proteinIds->{$query}->{ec}} > 0 ? 1 : 0;
	next if ( ! $subjectHasEc && ! $queryHasEc );
	
	my $exponentFromMantua = log($mantua)/log(10);
	$exponent += $exponentFromMantua;
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$query},$viableEc) && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		$blastPerEc->{$viableEc} = &addArrayElement($blastPerEc->{$viableEc},$exponent);
	    }
	    if ($subjectHasEc && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		$blastEvalues->{$query}->{$viableEc} = &addArrayElement($blastEvalues->{$query}->{$viableEc},$exponent);
	    }
	    if ($queryHasEc && &proteinHasThisEcNumber($proteinIds->{$query},$viableEc)) {
		$blastEvalues->{$subject}->{$viableEc} = &addArrayElement($blastEvalues->{$subject}->{$viableEc},$exponent);
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
    my ($proteinName,$proteinIds,$statsFh,$missing) = @_;

    if (! exists $proteinIds->{$proteinName}) {
	if (! exists $missing->{$proteinName}) {
	    print $statsFh "ERROR: The protein '$proteinName' does not exist in the original protein ids\n" if ($statsFh);
	    $missing->{$proteinName}=1;
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

sub getViableEcNumbers {
    my ($proteinIds,$minNumGenera,$minNumProteins,$statsFh) = @_;

    my $actualEcNumbers = &getUniqueEcNumbersFromProteins($proteinIds);
    my $allEcNumbers = &addPartialEcNumbers($actualEcNumbers);
    my $ecNumbersWithCounts = &getNumProteinsGeneraForEachEc($proteinIds,$allEcNumbers);
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
    my ($proteinIds) = @_;
    my $ecs;
    foreach my $id (keys %{$proteinIds}) {
	foreach my $ec ( @{$proteinIds->{$id}->{ec}} ) {
	    $ecs->{$ec} = 1;
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
    my ($proteinIds,$allEcNumbers) = @_;
    my $ecNumbersWithCounts;
    my $ecGenera;
    foreach my $id (keys %{$proteinIds}) {
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
    my ($a,$b,$c,$d) = split(/\./,$ec);
    return "" if ($c eq "-");
    return "$a.$b.-.-" if ($d eq "-");
    return "$a.$b.$c.-";
}

sub getBackgroundDomainCount {
    my ($domainCountFile,$dbh,$includeOld) = @_;
    my $domainCount;    
    if (-e $domainCountFile) {
	$domainCount = &readDomainCountFile($domainCountFile);
    } else {
	$domainCount = &countAllDomainsFromDatabase($dbh,$includeOld);
	&writeDomainCountFile($domainCount,$domainCountFile);
	$domainCount->{numProteins} = &getNumProteinsFromDatabase($dbh,$includeOld);
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
    my ($dbh,$includeOld) = @_;
    my $numProteins=0;

    my $query = $dbh->prepare(&numProteinsSql($includeOld));
    $query->execute();
    while (my($count) = $query->fetchrow_array()) {
	$numProteins = $count;
    }
    $query->finish();
    return $numProteins;
}


sub ecLengthStats {
    my ($proteinIds,$viableEcNumbers,$statsFh) = @_;

    # for each viable EC number, obtain array of protein lengths
    my $ecNumbers;
    foreach my $id (keys %{$proteinIds}) {
	next if ( scalar @{$proteinIds->{$id}->{ec}} == 0 );
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (proteinHasThisEcNumber($proteinIds->{$id},$viableEc)) {
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
	$ecStats->{$ec}->{max} = $noValues ? -1 :max($ecNumbers->{$ec});
	$ecStats->{$ec}->{median} = $noValues ? -1 :median($ecNumbers->{$ec});
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
    my ($proteinIds,$viableEcNumbers,$statsFh) = @_;

    my $domainToLetter = getDomainKey($proteinIds,$statsFh);
       
    my ($ecNumbers,$domainPerProtein) = &getAllDomainStringsPerEc($proteinIds,$viableEcNumbers,$domainToLetter);
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
    my ($proteinIds,$viableEcNumbers,$domainToLetter) = @_;
    my ($ecNumbers,$domainPerProtein);

    foreach my $id (keys %{$proteinIds}) {
	my $domainString = &getDomain($proteinIds->{$id},$domainToLetter);
	$domainPerProtein->{$id} = $domainString;
	next if (scalar @{$proteinIds->{$id}->{ec}} == 0);
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


sub domainScore {
    my ($id,$ec,$domainStatsPerEc,$domainPerProtein) = @_;

    my $score=0;
    my $idDomain = $domainPerProtein->{$id};
    return "-" if ($idDomain eq "-");
    foreach my $domainString ( keys %{$domainStatsPerEc->{$ec}->{domainString}} ) {
	if ($idDomain =~ /$domainString/) {
	    $score += $domainStatsPerEc->{$ec}->{domainString}->{$domainString}->{score};
	}
    }
    my $normalizedScore = $score / $domainStatsPerEc->{$ec}->{maxScore};
	#sprintf('%.1f', 100 * $score / $domainStatsPerEc->{$ec}->{maxScore});
    if ($normalizedScore > 0.75 ) {
	return "A";
    } elsif ($normalizedScore > 0.50) {
	return "B";
    } elsif ($normalizedScore > 0.25) {
	return "C";
    } else {
	return "D";
    }
}

sub lengthScore {
    my ($id,$ec,$proteinIds,$lengthStatsPerEc) = @_;

    my $idLength = $proteinIds->{$id}->{length};
    my $idDistanceFromMedian = abs($idLength - $lengthStatsPerEc->{$ec}->{median});
    my $tenPercentOfMedian = 0.1 * $lengthStatsPerEc->{$ec}->{median};
    
    if ($idDistanceFromMedian <= $tenPercentOfMedian ) {
	return "A";
    } elsif ($idDistanceFromMedian <= 2*$tenPercentOfMedian) {
	return "B";
    } elsif ($idLength >= $lengthStatsPerEc->{$ec}->{min} && $idLength <= $lengthStatsPerEc->{$ec}->{max}) {
	return "C";
    } else {
	return "D";
    }
}

sub blastScore {
    my ($id,$ec,$blastStatsPerEc,$blastStatsPerProtein) = @_;

    return "D" if (! exists $blastStatsPerProtein->{$id}->{$ec});   #this protein does not BLAST to any protein with an EC number

    my $idBlast = $blastStatsPerProtein->{$id}->{$ec}->{median};
    my $ecBlast =  $blastStatsPerEc->{$ec}->{median};
    $ecBlast = -181 if ($ecBlast == 0);  # this happens when protein with EC does not have blast partners
    my $tenPercentOfMedian = abs(0.1 * $ecBlast);

    if ($idBlast <= ($ecBlast+$tenPercentOfMedian) ) {
	return "A";
    } elsif ($idBlast <= ($ecBlast+2*$tenPercentOfMedian) ) {
	return "B";
    } elsif ($idBlast <= $blastStatsPerEc->{$ec}->{max} ) {
	return "C";
    } else {
	return "D";
    }
}

sub getProteinScores {
    my ($proteinIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$groupScoresFile) = @_;

    my $scores;
    foreach my $id (keys %{$proteinIds}) {
	foreach my $ec (keys %{$viableEcNumbers}) {
	    my $lengthScore = lengthScore($id,$ec,$proteinIds,$lengthStatsPerEc);
	    my $blastScore = blastScore($id,$ec,$blastStatsPerEc,$blastStatsPerProtein);
	    my $domainScore = domainScore($id,$ec,$domainStatsPerEc,$domainPerProtein);
	    my $scoreString = $lengthScore.$blastScore.$domainScore;
	    next if ($scoreString =~ /[D]/);
	    $scores->{$id}->{$ec} = $scoreString;
	}
    }
    &deletePartialEcWithWorseScore($scores);
    &printEcScores($scores,$groupScoresFile);
    return $scores;
}

sub deletePartialEcWithWorseScore {
    my ($scores) = @_;
    foreach my $id (keys %{$scores}) {
	my %toDelete;
	foreach my $ec (keys %{$scores->{$id}}) {
	    my $parentEc = getParent($ec);
	    if ($parentEc && exists $scores->{$id}->{$parentEc}) {
		if (&scoreToDigit($scores->{$id}->{$ec}) >= &scoreToDigit($scores->{$id}->{$parentEc})) {
		    $toDelete{$parentEc} = 1;      # if parent has same or worse score, then delete parent
		}
	    }
	}
	foreach my $ec (keys %toDelete) {
	    delete $scores->{$id}->{$ec};
	}
    }
}

sub scoreToDigit {
    my ($letterScore) = @_;
    $letterScore =~ tr/ABCD-/43204/;
    my @digits = split("",$letterScore);
    my $sum = &sum(\@digits);
    return $sum;
}

sub printEcScores {
    my ($scores,$groupScoresFile) = @_;
    open(my $scoreFh,">",$groupScoresFile) || die "Cannot open file '$groupScoresFile' for writing";
    foreach my $id (keys %{$scores}) {
	foreach my $ec (keys %{$scores->{$id}}) {
	    print $scoreFh "$id\t$ec\t$scores->{$id}->{$ec}\n";
	}
    }
    close $scoreFh;
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

    my $stringCounter = initializeStringCounter($stringLength);
    my @domainArray = sort { $domains->{$b} <=> $domains->{$a} } keys %{$domains};
    
    print $statsFh "DOMAIN\tSTRING\n";
    for (my $currentDomain=0; $currentDomain<$numDomains; $currentDomain++) {
	my $currentString = makeStringFromCounter($stringCounter,\@alphabet);
	$domains->{$domainArray[$currentDomain]} = $currentString;
	print $statsFh "$domainArray[$currentDomain]\t$currentString\n";
	$stringCounter = increaseStringCounter($stringCounter,$numCharacters);
    }
    print $statsFh "\n";
    
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
    my ($proteinIds,$proteinInfoFile) =@_;

    open(OUT,">",$proteinInfoFile) || die "Cannot open '$proteinInfoFile' for writing";
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
    my ($proteinInfoFile,$includeOld) = @_;

    my $proteinIds;
    open(IN,$proteinInfoFile) || die "Cannot open '$proteinInfoFile' for reading";
    while (my $line = <IN>) {
	chomp($line);
	next if ($line !~ /^OG/);
	my ($group,$id,$taxon,$length,$domains,$product,$ecs) = split("\t",$line);
	next if ($includeOld && $id =~ /-old\|/);
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
    return $proteinIds;
}


sub getProteinInfo {
    my ($group,$includeOld,$dbh,$proteinInfoFile) = @_;

    my $proteinIds;

    if ($proteinInfoFile && -e $proteinInfoFile) {
	$proteinIds = &readProteinInfoFile($proteinInfoFile,$includeOld);
    } else {
	$proteinIds = &getProteinsFromDatabase($proteinIds,$group,$includeOld,$dbh);
	$proteinIds = &getEcsFromDatabase($proteinIds,$includeOld,$dbh);
	$proteinIds = &getDomainsFromDatabase($proteinIds,$includeOld,$dbh);
	&writeProteinInfoFile($proteinIds,$proteinInfoFile) if ($proteinInfoFile);
    }
    return $proteinIds;
}

sub getProteinsFromDatabase {
    my ($proteinIds,$group,$includeOld,$dbh) = @_;

    my $query = $dbh->prepare(&proteinsSql($group));
    
    $query->execute();
    while (my($id,$product,$length,$corePeripheral,$group,$taxon) = $query->fetchrow_array()) {
	next if ($includeOld && $id =~ /-old\|/);
	$proteinIds->{$id}->{product} = $product;
	$proteinIds->{$id}->{length} = $length;
	$proteinIds->{$id}->{corePeripheral} = $corePeripheral;
	$proteinIds->{$id}->{group} = $group;
	$proteinIds->{$id}->{taxon} = $taxon;
	$proteinIds->{$id}->{ec} = [];
	$proteinIds->{$id}->{domain} = [];
    }    
    $query->finish();
    return $proteinIds;
}

sub createIdString {
    my ($proteinIds,$includeOld) = @_;
    my @ids;
    foreach my $id (keys %{$proteinIds}) {
	next if ($includeOld && $id =~ /-old\|/);
	push @ids, $id;
    }
    my $idString = join("','",@ids);
    return "('".$idString."')";
}

sub getEcsFromDatabase {
    my ($proteinIds,$includeOld,$dbh) = @_;

    my $idString = createIdString($proteinIds,$includeOld);

    my $query = $dbh->prepare(&ecsSql($idString));
    
    $query->execute();
    while (my($id,$ecString) = $query->fetchrow_array()) {
	die "The protein '$id' was not found in group" if (! exists $proteinIds->{$id});
	$ecString =~ s/ //g;
	my @multipleEc = split(/[;,]/,$ecString);
	foreach my $ec ( @multipleEc) {
	    die "Incorrect EC number '$ec' for protein '$id'" if (! &validEcNumber($ec));
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
    my ($proteinIds,$includeOld,$dbh) = @_;

    my $idString = createIdString($proteinIds,$includeOld);

    my $query = $dbh->prepare(&domainsSql($idString));

    $query->execute();
    while (my($id,$domain) = $query->fetchrow_array()) {
	die "The protein '$id' was not found in group" if (! exists $proteinIds->{$id});
	push @{$proteinIds->{$id}->{domain}}, $domain;
    }
    $query->finish();
    return $proteinIds;
}

sub countAllDomainsFromDatabase {
    my ($dbh,$includeOld) = @_;
    my $query = $dbh->prepare(&allDomainsSql);
    $query->execute();
    my $domainCount;
    my %seen;
    while (my($id,$domain) = $query->fetchrow_array()) {
	next if ($includeOld && $id =~ /-old\|/);
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
   
sub getGroupsFromProteins {
    my ($proteinIds) = @_;
    my $groups;
    foreach my $protein (keys %{$proteinIds}) {
	if ( exists $groups->{$proteinIds->{$protein}->{group}} ) {
	    push @{$groups->{$proteinIds->{$protein}->{group}}}, $protein;
	} else {
	    $groups->{$proteinIds->{$protein}->{group}} = [$protein];
	}
    }
    return $groups;
}

sub getGroupsFromDatabase {
    my ($minNumProteinsWithEc,$dbh) = @_;
    my %groups;
    
    my $query = $dbh->prepare(&groupsSql($minNumProteinsWithEc));

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
    my ($idString) = @_;
    return "SELECT full_id,accession FROM ApidbTuning.DomainAssignment
            WHERE full_id IN $idString ORDER BY full_id,start_min";
}

sub ecsSql {
    my ($idString) = @_;
    return "SELECT eas.secondary_identifier,ec.ec_number
            FROM SRes.EnzymeClass ec, DoTS.AASequenceEnzymeClass aaec,
                 dots.ExternalAASequence eas
            WHERE ec.enzyme_class_id = aaec.enzyme_class_id AND aaec.aa_sequence_id = eas.aa_sequence_id
                  AND eas.secondary_identifier IN $idString";
}

sub proteinsSql {
    my ($group) = @_;
    return "SELECT full_id,product,length,core_peripheral,group_name,taxon_name                                     
            FROM ApidbTuning.SequenceAttributes WHERE group_name='$group'";
}

sub groupsSql {
    my ($minNumProteinsWithEc) = @_;
    return "SELECT group_name
            FROM (SELECT group_name,count(ec_numbers) as num_proteins                         
                  FROM ApidbTuning.SequenceAttributes sa
                  WHERE ec_numbers IS NOT NULL
                  GROUP BY group_name)
            WHERE num_proteins >= $minNumProteinsWithEc";
}

sub blastSql {
    my ($group) = @_;
    return "SELECT ssg.query_id, ssg.subject_id, ssg.evalue_mant, ssg.evalue_exp
            FROM apidb.similarSequencesGroupCore ssg, apidb.orthologGroup og
            WHERE ssg.ortholog_group_id=og.ortholog_group_id AND og.name='$group'";
}

sub numProteinsSql {
    my ($includeOld) = @_;
    my $whereClause = $includeOld ? "WHERE secondary_identifier NOT LIKE '%-old|%'" : "";
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
    mkdir($dir) || die "Unable to create directory '$dir'" unless (-e $dir);
}
