#!/usr/bin/perl
use warnings;
use strict;
use Data::Dumper;
use lib "$ENV{GUS_HOME}/lib/perl";
use CBIL::Util::PropertySet;
use DBI;

# example command line:  perl prediction.pl /home/markhick/ec 'no'
# good groups to study: OG6_100435, OG6_101725

my $outputDirectory = $ARGV[0];
my $includeOld = $ARGV[1];

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

my $dbh = getDbHandle();

my $groupsToTest = getGroupsFromDatabase($minNumProteinsWithEc,$dbh);

foreach my $group (keys %{$groupsToTest}) {
    my $outputFileStats = "$outputDirectory/$group/stats.txt";
    my $outputProteinFile = "$outputDirectory/$group/proteins.txt";
    my $groupScoresFile = "$outputDirectory/$group/scores.txt";
    open(my $outFh,">",$outputFileStats) || die "Cannot open $outputFileStats for writing\n";

    my $proteinIds = &getProteinInfo($group,$includeOld,$dbh,$outputProteinFile);
    my $viableEcNumbers = &getViableEcNumbers($proteinIds,$minNumGenera,$minNumProteins,$outFh);
    
    my $domainStatsPerEc;
    my $domainPerProtein;
    if (&numProteinsWithDomainAndEc($proteinIds) > 1) {
	($domainStatsPerEc,$domainPerProtein) = &ecDomainStats($proteinIds,$viableEcNumbers,$outFh);
    }

    my $lengthStatsPerEc = &ecLengthStats($proteinIds,$viableEcNumbers,$outFh);

    my ($blastStatsPerEc,$blastStatsPerProtein) = &ecBlastStats($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$outFh);

    &outputProteinScores($proteinIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$groupScoresFile);

}

$dbh->disconnect();
exit;




################################  SUBROUTINES  ########################################


sub ecBlastStats {
    my ($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$outFh) = @_;

    my ($blastEvalues,$blastPerEc) = &readBlastEvaluesFromDatabase($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$outFh);
    my @orderedViableEcs = sort keys %{$viableEcNumbers};
    my $blastStatsPerEc = &calculateBlastStatsPerEc(\@orderedViableEcs,$blastPerEc,$outFh);
    my $blastStatsPerProtein = &calculateBlastStatsPerProtein(\@orderedViableEcs,$blastEvalues,$outFh);
    
    return ($blastStatsPerEc,$blastStatsPerProtein);
}

sub readBlastEvaluesFromDatabase {
    my ($dbh,$group,$proteinIds,$viableEcNumbers,$includeOld,$outFh) = @_;

    my $blastEvalues;    # id -> ec -> (-5,-6.4,-150.3)
    my $blastPerEc;        # ec -> (-5,-6.4,-150.3)  only if both proteins have EC

    my $missing;
    my $query = $dbh->prepare(<<SQL);
SELECT ssg.query_id, ssg.subject_id, ssg.evalue_mant, ssg.evalue_exp,
FROM apidb.similarSequencesGroupCore ssg, apidb.orthologGroup og
WHERE ssg.ortholog_group_id=og.ortholog_group_id AND og.name='$group';
SQL
    
    $query->execute();
    while (my($query,$subject,$manuta,$exponent) = $query->fetchrow_array()) {
	next if ($includeOld =~ /[Nn]o/ && ($query =~ /-old\|/ || $subject =~ /-old\|/));
	next if (&proteinDoesNotExist($subject,$proteinIds,$outFh,$missing));
	next if (&proteinDoesNotExist($query,$proteinIds,$outFh,$missing));
	
	my $subjectHasEc =  scalar @{$proteinIds->{$subject}->{ec}} > 0 ? 1 : 0;
	my $queryHasEc =  scalar @{$proteinIds->{$query}->{ec}} > 0 ? 1 : 0;
	next if ( ! $subjectHasEc && ! $queryHasEc );
	
	my $exponentFromMantua = log($mantua)/log(10);
	$exponent += $exponentFromMantua;
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (&proteinHasThisEcNumber($proteinIds->{$query},$viableEc) && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		&addArrayElement($blastPerEc->{$viableEc},$exponent);
	    }
	    if ($subjectHasEc && &proteinHasThisEcNumber($proteinIds->{$subject},$viableEc)) {
		&addArrayElement($blastEvalues->{$query}->{$viableEc},$exponent);
	    }
	    if ($queryHasEc && &proteinHasThisEcNumber($proteinIds->{$query},$viableEc)) {
		&addArrayElement($blastEvalues->{$subject}->{$viableEc},$exponent);
	    }
	}
    }
    $query->finish();

    return ($blastEvalues,$blastPerEc);
}

sub calculateBlastStatsPerEc {
    my ($orderedEcs,$blastPerEc,$outFh) = @_;

    print $outFh "\nBLAST_STATISTICS\n";
    print $outFh "ec_number\tnum_values\tminimum\tmaximum\tmedian\tmean\tstd_dev\n";
    my $blastStatsPerEc;
    foreach my $ec (@{$orderedEcs}) {
	print $outFh "$ec";
	my $noValues;
	$noValues = 1 if (! exists $blastPerEc->{$ec});   # this EC does not have any blast partners
	$blastStatsPerEc->{$ec}->{numValues} = $noValues ? 0 : scalar @{$blastPerEc->{$ec}};
	print  $outFh "\t$blastStatsPerEc->{$ec}->{numValues}";
	$blastStatsPerEc->{$ec}->{min} = $noValues ? 0 : sprintf('%.1f',min($blastPerEc->{$ec}));
	print  $outFh "\t$blastStatsPerEc->{$ec}->{min}";
	$blastStatsPerEc->{$ec}->{max} = $noValues ? 0 : sprintf('%.1f',max($blastPerEc->{$ec}));
	print  $outFh "\t$blastStatsPerEc->{$ec}->{max}";
	$blastStatsPerEc->{$ec}->{median} = $noValues ? 0 : sprintf('%.1f',median($blastPerEc->{$ec}));
	print  $outFh "\t$blastStatsPerEc->{$ec}->{median}";
	my ($mean,$sd) = meanSd($blastPerEc->{$ec}) if (! $noValues);
	$blastStatsPerEc->{$ec}->{mean} = $noValues ? 0 : sprintf('%.1f',$mean);
	print  $outFh "\t$blastStatsPerEc->{$ec}->{mean}";
	$blastStatsPerEc->{$ec}->{sd} = $noValues ? 0 : sprintf('%.1f',$sd);
	print  $outFh "\t$blastStatsPerEc->{$ec}->{sd}";
	print $outFh "\n";
    }
    print $outFh "\n";
    return $blastStatsPerEc;
}

sub calculateBlastStatsPerProtein {
    my ($orderedEcs,$blastEvalues,$outFh);

    print $outFh "GENE\t";
    foreach my $ec (@{$orderedEcs}) {
	print $outFh "$ec\t\t\t\t\t\t";
    }
    print $outFh "\n";
    foreach my $ec (@{$orderedEcs}) {
	print $outFh "\tnumber_values\tminimum\tmaximum\tmedian\tmean\tstd_dev";
    }
    print $outFh "\n";
    
    my $blastStatsPerProtein;
    foreach my $id (keys %{$blastEvalues}) {
	print $outFh "$id";
	foreach my $viableEc ( @{$orderedEcs} ) {
	    my $noValues;
	    $noValues = 1 if (! exists $blastEvalues->{$id}->{$viableEc});   # this id does not have blast partner containing this EC number
	    $blastStatsPerProtein->{$viableEc}->{$id}->{numValues} = $noValues ? 0 : scalar @{$blastEvalues->{$id}->{$viableEc}};
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{numValues}";
	    $blastStatsPerProtein->{$viableEc}->{$id}->{min} = $noValues ? 0 : sprintf('%.1f',min($blastEvalues->{$id}->{$viableEc}));
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{min}";
	    $blastStatsPerProtein->{$viableEc}->{$id}->{max} = $noValues ? 0 : sprintf('%.1f',max($blastEvalues->{$id}->{$viableEc}));
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{max}";
	    $blastStatsPerProtein->{$viableEc}->{$id}->{median} = $noValues ? 0 : sprintf('%.1f',median($blastEvalues->{$id}->{$viableEc}));
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{median}";
	    my ($mean,$sd) = meanSd($blastEvalues->{$id}->{$viableEc}) if (! $noValues);
	    $blastStatsPerProtein->{$viableEc}->{$id}->{mean} = $noValues ? 0 : sprintf('%.1f',$mean);
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{mean}";
	    $blastStatsPerProtein->{$viableEc}->{$id}->{sd} = $noValues ? 0 : sprintf('%.1f',$sd);
	    print  $outFh "\t$blastStatsPerProtein->{$viableEc}->{$id}->{sd}";
	}
	print $outFh "\n";
    }
    print $outFh "\n";
    return $blastStatsPerProtein;
}


sub proteinDoesNotExist {
    my ($proteinName,$proteinIds,$outFh,$missing) = @_;

    if (! exists $proteinIds->{$proteinName}) {
	if (! exists $missing->{$proteinName}) {
	    print $outFh "ERROR: The protein '$proteinName' does not exist in the original protein ids\n";
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
    my ($proteinIds,$minNumGenera,$minNumProteins,$outFh) = @_;

    my $actualEcNumbers = &getUniqueEcNumbersFromProteins($proteinIds);
    my $allEcNumbers = &addPartialEcNumbers($actualEcNumbers);
    my $ecNumbersWithCounts = &getNumProteinsGeneraForEachEc($proteinIds,$allEcNumbers);
    &deletePartialEcNumbers($ecNumbersWithCounts);
    &deleteEcNumbersBelowMin($ecNumbersWithCounts,$minNumProteins,$minNumGenera);

    print $outFh "EC_NUMBER\tNUM_PROTEINS\tNUM_GENERA\n";
    foreach my $ec (sort keys %{$ecNumbersWithCounts}) {
	print $outFh "$ec\t$ecNumbersWithCounts->{$ec}->{numProteins}\t$ecNumbersWithCounts->{$ec}->{numGenera}\n";
    }
    print $outFh "\n";
    
    return $ecNumbersWithCounts;
}

sub getUniqueEcNumbersFromProteins {
    my ($proteinIds) = @_;
    my $ecs;
    foreach my $id (keys %{$proteinIds}) {
	foreach my $ec ( @{$proteinIds->{$id}->{ec}} ) {
	    $ecs->{$ec};
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
	    if &proteinHasThisEcNumber($proteinIds->{$id},$ec) {
		$ecNumbersWithCounts->{$ec}->{numProteins}++;
		&addArrayElement($ecGenera->{$ec},$genus);
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
    if (exists $arrayRef) {
	push @{$arrayRef}, $element;
    } else {
	$arrayRef = [$element];
    }
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
	my ($a,$b,$c,$d) = split(/\./,$ec);
	next if ($c eq "-" && $d eq "-");   # there are no parents of this one
	$parentEc = getParent($ec);
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
    my ($domainCountFile,$domainFile,$proteinFile,$includeOld) = @_;

    my $domainCount;
    
    if (-e $domainCountFile) {
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

    } else {
	my $domainCount->{numProteins} = getNumProteins($proteinFile,$includeOld);

	open(IN,$domainFile) || die "Cannot open $domainFile\n";
	my %seen;
	while (<IN>) {
	    my $line = $_;
	    chomp $line;
	    $line =~ s/"//g;
	    next if ($line !~ /^[^\|]{4,8}\|/);
	    my ($id,$domain) = split("\t",$line);
	    next if ($includeOld =~ /[Nn]o/ && $id =~ /-old\|/);
	    next if ($seen{$id.$domain});
	    $seen{$id.$domain} = 1;
	    $domainCount->{domain}->{$domain}++;
	}
	close IN;

	open(OUT,">",$domainCountFile) || die "Cannot open $domainCountFile for writing\n";
	print OUT "DOMAIN\tNUM_PROTEINS\n";
	print OUT "all_proteins\t$domainCount->{numProteins}\n";
	foreach my $domain (keys %{$domainCount->{domain}}) {
	    print OUT "$domain\t$domainCount->{domain}->{$domain}\n";
	}
	close OUT;
    }
    return $domainCount;
}


sub getNumProteins {
    my ($proteinFile,$includeOld) = @_;
    my $numProteins=0;

    open(IN,$proteinFile) || die "Cannot open $proteinFile\n";
    while (<IN>) {
	my $line = $_;
	chomp $line;
	$line =~ s/"//g;
	next if ($includeOld =~ /[Nn]o/ && $line =~ /^[^\|]{4}-old\|/);
        $numProteins++ if ($line =~ /^[^\|]{4,8}\|/);
    }
    close IN;
    return $numProteins;
}


sub ecLengthStats {
    my ($proteinIds,$viableEcNumbers,$outFh) = @_;

    # for each viable EC number, obtain array of protein lengths
    my $ecNumbers;
    foreach my $id (keys %{$proteinIds}) {
	next if ( scalar @{$proteinIds->{$id}->{ec}} == 0 );
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (proteinHasThisEcNumber($proteinIds->{$id},$viableEc)) {
		&addArrayElement($ecNumbers->{$viableEc},$proteinIds->{$id}->{length});
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

    print $outFh "EC_NUMBER\tNUM_PROTEINS\tMIN_LENGTH\tMAX_LENGTH\t_MEDIAN_LENGTH\tMEAN_LENGTH\tSTD_DEV_LENGTH\n";
    foreach my $ec (sort keys %{$ecStats}) {
	    print $outFh "$ec\t$ecStats->{$ec}->{numProteins}";
	    print $outFh "\t$ecStats->{$ec}->{min}\t$ecStats->{$ec}->{max}";
	    print $outFh "\t$ecStats->{$ec}->{median}\t$ecStats->{$ec}->{mean}";
	    print $outFh "\t$ecStats->{$ec}->{sd}\n";
    }
    print $outFh "\n";
    return $ecStats;
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
    my $sum = 0;
    my $numSamples = scalar @{$arrayRef};
    foreach my $number (@{$arrayRef}) {
	$sum += $number;
    }
    my $mean = $sum/$numSamples;

    $sum=0;
    foreach my $number (@{$arrayRef}) {
	$sum += ($number-$mean)**2;
    }
    my $sd = sqrt($sum/$numSamples);

    return ($mean,$sd);
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
    my ($a1,$b1,$c1,$d1) = split(".",$ecNumber);
    foreach my $currentEc ( @{$protein->{ec}} ) {
	my ($a2,$b2,$c2,$d2) = split(".",$currentEc);
	$c2 = "-" if ($c1 eq "-");
	$d2 = "-" if ($d1 eq "-");
	if ($a1 eq $a2 && $b1 eq $b2 && $c1 eq $c2 && $d1 eq $d2) {
	    return 1;
	}
    }
    return 0;
}

sub ecDomainStats {
    my ($proteinIds,$viableEcNumbers,$outFh) = @_;

    my $domainToLetter = getDomainKey($proteinIds,$outFh);
       
    # for each viable EC number, determine number of proteins with each domain string and sub-string
    my $ecNumbers;    # ec -> domain -> count
    my $domainPerProtein;  # id -> domain
    print $outFh "GENE\tDOMAINS\n";
    foreach my $id (keys %{$proteinIds}) {
	next if (scalar @{$proteinIds->{$id}->{ec}} == 0);
	my $domainString = getDomain($proteinIds->{$id},$domainToLetter);
	$domainPerProtein->{$id} = $domainString;
	print $outFh "$id\t$domainString\n";
	my $domains = getAllPossibleCombinations($domain,"");
	foreach my $viableEc ( keys %{$viableEcNumbers} ) {
	    if (proteinHasThisEcNumber($proteinIds->{$id},$viableEc)) {
		foreach my $domainString (keys %{$domains}) {
		    $ecNumbers->{$viableEc}->{domainString}->{$domainString}->{count}++;
		}
	    }
	}
    }
    print $outFh "\n";
	
    # for each EC number, calculate total number of proteins
    # for each string, calculate score
    foreach my $ec (keys %{$ecNumbers}) {
	$ecNumbers->{$ec}->{numProteins} = $viableEcNumbers->{$ec}->{numProteins};
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    $ecNumbers->{$ec}->{domainString}->{$string}->{score} =  $ecNumbers->{$ec}->{domainString}->{$string}->{count} / $viableEcNumbers->{$ec}->{numProteins};
	}
    }

    # for each EC number, calculate max score
    foreach my $ec (keys %{$ecNumbers}) {
	$ecNumbers->{$ec}->{maxScore} = 0;
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    $ecNumbers->{$ec}->{maxScore} += $ecNumbers->{$ec}->{domainString}->{$string}->{score};
	}
    }

    print $outFh "EC_NUMBER\tDOMAIN_STRING\tNUM_PROTEINS\tSCORE\n";
    foreach my $ec (sort keys %{$ecNumbers}) {
	print $outFh "$ec\t--NUM PROTEINS--\t$ecNumbers->{$ec}->{numProteins}\t$ecNumbers->{$ec}->{maxScore}\n";
	foreach my $string ( keys %{$ecNumbers->{$ec}->{domainString}} ) {
	    print $outFh "$ec\t$string";
	    print $outFh "\t$ecNumbers->{$ec}->{domainString}->{$string}->{count}";
	    print $outFh "\t$ecNumbers->{$ec}->{domainString}->{$string}->{score}\n";
	}
    }
    print $outFh "\n";

    return ($ecNumbers,$domainPerProtein);
}

sub getDomain {
    my ($proteinRef,$domainToLetter) = @_;
    my $domain = "";
    if (scalar @{$proteinRef->{domain}} > 0) {
	my @domainLetters = map { $domainToLetter->{$_} } @{$proteinRef->{domain}};
	$domain = join("",@domainLetters);
    }
    $domain = "-" if ($domain eq "");
    return $domain;
}


sub domainScore {
    my ($id,$ec,$domainStatsPerEc,$domainPerProtein) = @_;

    my $score=0;
    my $idDomain = $domainPerProtein->{$id};
    return "" if ($idDomain eq "");
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

    return "D" if (! exists $blastStatsPerProtein->{$ec}->{$id});   #this protein does not BLAST to any protein with an EC number

    my $idBlast = $blastStatsPerProtein->{$ec}->{$id}->{median};
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

sub outputProteinScores {
    my ($proteinIds,$viableEcNumbers,$domainStatsPerEc,$domainPerProtein,$lengthStatsPerEc,$blastStatsPerEc,$blastStatsPerProtein,$groupScoresFile) = @_;

    my @ecArray = reverse sort keys %{$viableEcNumbers};

    open(OUT,">",$groupScoresFile) || die "Cannot open file '$groupScoresFile' for writing";
    foreach my $id (keys %{$proteinIds}) {
	my $pastEc = "";
	foreach my $ec (@ecArray) {
	    if ($ec =~ /^([^-]+)-/) {       # skip less specific EC number like 1.2.-.- if there is more specific like 1.2.3.- or 1.2.3.4
		my $firstDigits = $1;
		if ($pastEc =~ /^($firstDigits)/) {
		    next;
		}
	    }
	    my $lengthScore = lengthScore($id,$ec,$proteinIds,$lengthStatsPerEc);
	    my $blastScore = blastScore($id,$ec,$blastStatsPerEc,$blastStatsPerProtein);
	    my $domainScore = domainScore($id,$ec,$domainStatsPerEc,$domainPerProtein);
	    my $scoreString = $lengthScore.$blastScore.$domainScore;
	    next if ($scoreString =~ /[CD]/);
	    $pastEc = $ec;
	    print OUT "$id\t$ec ($scoreString)\n";
	}
    }
    close OUT;
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
    my ($proteinIds,$outFh) = @_;
    
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
    
    print $outFh "DOMAIN\tSTRING\n";
    for (my $currentDomain=0; $currentDomain<$numDomains; $currentDomain++) {
	my $currentString = makeStringFromCounter($stringCounter,\@alphabet);
	$domains->{$domainArray[$currentDomain]} = $currentString;
	print $outFh "$domainArray[$currentDomain]\t$currentString\n";
	$stringCounter = increaseStringCounter($stringCounter,$numCharacters);
    }
    print $outFh "\n";
    
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
	$ecs[0] = "." if (scalar @ecs == 0);
	my $ec = join(",",@ecs);
	my @domains = @{$proteinIds->{$id}->{domain}};
	$domains[0] = "." if (scalar @domains == 0);
	my $domain = join(",",@domains);
	print OUT "$group\t$id\t$taxon\t$length\t$domain\t$product\t$ec\n";
    }
    close OUT; 
}

sub readProteinInfoFile {
    my ($proteinInfoFile,$includeOld) =@_;

    my $proteinIds;
    open(IN,$proteinInfoFile) || die "Cannot open '$proteinInfoFile' for reading";
    while (my $line = <IN>) {
	chomp($line);
	next if ($line !~ /^OG/);
	my ($group,$id,$taxon,$length,$domains,$product,$ecs) = split("\t",$line);
	next if ($includeOld =~ /[Nn]o/ && $id =~ /-old\|/);
	$proteinIds->{$id}->{group} = $group;
	$proteinIds->{$id}->{taxon} = $taxon;
	$proteinIds->{$id}->{length} = $length;
	$proteinIds->{$id}->{product} = $product;
	$ecs = $ecs eq "." ? "" : $ecs;
	$proteinIds->{$id}->{ec} = [split(",",$ecs)];
	$domains = $domains eq "." ? "" : $domains;
	$proteinIds->{$id}->{domain} = [split(",",$domains)];
    }
    close IN;
    return $proteinIds;
}


sub getProteinInfo {
    my ($group,$includeOld,$dbh,$proteinInfoFile) = @_;

    my $proteinIds;

    if (-e $proteinInfoFile) {
	$proteinIds = readProteinInfoFile($proteinInfoFile,$includeOld);
    } else {
	$proteinIds = getProteinsFromDatabase($proteinIds,$group,$includeOld,$dbh);
	$proteinIds = getEcsFromDatabase($proteinIds,$includeOld,$dbh);
	$proteinIds = getDomainsFromDatabase($proteinIds,$includeOld,$dbh);
	writeProteinInfoFile($proteinIds,$proteinInfoFile);
    }
    return $proteinIds;
}

sub getProteinsFromDatabase {
    my ($proteinIds,$group,$includeOld,$dbh) = @_;

    my $query = $dbh->prepare(<<SQL);
SELECT full_id,product,length,core_peripheral,group_name,taxon_name                                     
FROM ApidbTuning.SequenceAttributes sa
WHERE group_name='$group'
SQL
    
    $query->execute();
    while (my($id,$product,$length,$corePeripheral,$group,$taxon) = $query->fetchrow_array()) {
	next if ($includeOld =~ /[Nn]o/ && $id =~ /-old\|/);
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


sub getEcsFromDatabase {
    my ($proteinIds,$includeOld,$dbh) = @_;

    my @ids;
    foreach my $id (keys %{$proteinIds}) {
	next if ($includeOld =~ /[Nn]o/ && $id =~ /-old\|/);
	push @ids, $id;
    }
    my $idString = join("','",@ids);
    $idString = "('".$idString."')";

    my $query = $dbh->prepare(<<SQL);
SELECT eas.secondary_identifier,ec.ec_number
FROM SRes.EnzymeClass ec,
     DoTS.AASequenceEnzymeClass aaec,
     dots.ExternalAASequence eas
WHERE ec.enzyme_class_id = aaec.enzyme_class_id
      AND aaec.aa_sequence_id = eas.aa_sequence_id
      AND eas.secondary_identifier IN $idString
SQL
    
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
	my @sortedEcs = sort @{$proteinIds->{$id}->{ec}};
	@{$proteinIds->{$id}->{ec}} = @sortedEcs;
    }
    
    return $proteinIds;
}

sub getDomainsFromDatabase {
    my ($proteinIds,$includeOld,$dbh) = @_;

    my @ids;
    foreach my $id (keys %{$proteinIds}) {
	next if ($includeOld =~ /[Nn]o/ && $id =~ /-old\|/);
	push @ids, $id;
    }
    my $idString = join("','",@ids);
    $idString = "('".$idString."')";

    my $query = $dbh->prepare(<<SQL);
SELECT full_id,accession
FROM ApidbTuning.DomainAssignment
WHERE full_id IN $idString
ORDER BY full_id,start_min
SQL

    $query->execute();
    while (my($id,$domain) = $query->fetchrow_array()) {
	die "The protein '$id' was not found in group" if (! exists $proteinIds->{$id});
	push @{$proteinIds->{$id}->{domain}}, $domain;
    }
    $query->finish();
    return $proteinIds;
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
    
    my $query = $dbh->prepare(<<SQL);
SELECT group_name
FROM (SELECT group_name,count(ec_numbers) as num_proteins                         
      FROM ApidbTuning.SequenceAttributes sa
      WHERE ec_numbers IS NOT NULL
      GROUP BY group_name)
WHERE num_proteins >= $minNumProteinsWithEc
SQL

    $query->execute();
    while (my($group) = $query->fetchrow_array()) {
        $groups{$group} = 1;
    }
    $query->finish();
    return \%groups;
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
