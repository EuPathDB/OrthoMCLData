package OrthoMCLData::Load::Plugin::InsertGroupKeywords;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclGroupKeyword;

use ApiCommonData::Load::Util;
use Data::Dumper;

require Exporter;

my $argsDeclaration =
[
];


my $purpose = <<PURPOSE;
Calculate the relevant keywords for each ortholog group in the DB, and insert the keywords (with frequencies) into the OrthomclGroupKeyword table.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert the group keywords into the OrthomclGroupKeyword table  
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclGroupKeyword,
TABLES_AFFECTED

my $tablesDependedOn = <<TABLES_DEPENDED_ON;
ApiDB.OrthologGroup,

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

    # read sequence descriptions from db per group
    my $dbh = $self->getQueryHandle();
    my $sql = "SELECT ogs.ortholog_group_id, eas.description
               FROM ApiDB.OrthologGroupAaSequence ogs,
                    DoTS.ExternalAaSequence eas
               WHERE ogs.aa_sequence_id = eas.aa_sequence_id
               ORDER BY ogs.ortholog_group_id ASC";
    my $stmt = $dbh->prepare($sql);
    
    $stmt->execute();

    my $cur_group;
    my @lines;

    while (my @data = $stmt->fetchrow_array()) {
	if ($cur_group && $cur_group ne $data[0]) {
	    my %keywords = FunKeyword(\@lines);
	    submitKeywords(\%keywords, $cur_group);
	    @lines = ();
	}
	elsif (!$cur_group || $cur_group ne $data[0]) {
	    $cur_group = $data[0];
	}
	else {
	    $self->error("Unexpected error:\tcur_group=$cur_group\tdata[0]=$data[0]");
	}
	push(@lines, @data[1]);   
    }

    return "Done adding group keywords.";
}

sub submitKeywords {
    my ($self, %keywords, $group_id) = @_;

    foreach my $k (keys %keywords) {
	my $keyword = GUS::Model::ApiDB::OrthomclGroupKeyword->new();
	
	$keyword->setOrthologGroupId($group_id);
	$keyword->setKeyword($k);
	$keyword->setFrequency($keywords{$k});

	$keyword->submit();
    }
}

sub undoTables {
    my ($self) = @_;
    
    return ('ApiDB.OrthomclGroupKeyword',
	    );
}

# ----------------------------------------------------------------------

my @noword=qw(
    ensembl pfam swiss gi
    to of and in the by at with from
    cerevisiae saccharomyces arabidopsis thaliana mus musculus sapiens homo rattus norvegicus gallus plasmodium
    no not
    a the
    some
    contains involved -related related -like like unclassified expressed
    predicted putative ambiguous unknown similar probable possible potential
    family
    identical highly weakly likely nearly
    fragment
);

our @dashword = qw(
    dependent terminal containing specific associated directed rich
    transporting binding reducing conjugating translocating interacting
);

our @nosingleword = qw(
    protein proteins gene genes cds product peptide polypeptide enzyme sequence molecule factor
    function functions subfamily superfamily group profile
    similarity similarities homology homolog conserved 
    type domain domains chain class component components member motif terminal subunit box
    alpha beta delta gamma sigma lambda epsilon
    specific associated
    small
	precursor
);
our @capitalword = qw(
		DNA RNA ATP ADP AMP GTP
		ABC
		ATPase GTPase
		III
		HIV
		UV
		Rab
		NH2
		SH3 SH2 WD LIM PPR
		Na Fe
		CoA
	);

my %word_filter;
foreach (@nosingleword) {$word_filter{nosingleword}->{$_}=1;}
foreach (@capitalword) {$word_filter{capitalword}->{$_}=1;}


sub FunKeyword {
	my @funlines = @{$_[0]};

	my $n_words=10; # define the max number of words for keyword phrases
	my $freq_cutoff=0.5; # define the smallest frequency cutoff for returned keyword or keyword phrases
	my $return_n_words=20; # define the max number of total words for returned keyword or keyword phrases

	my $total_funlines=0;
	my %freq;

	NEXTLINE: foreach my $line (@funlines) {
	    next NEXTLINE if (!$line);
	    
		# removing some punctuation marks of the line
		$line=punctuation_mark($line);
		# dealing with the upper/lower cases
		# dealing with the cases that "Protein" and "protein" are actually the same word
		$line=linecase($line);
		#filtering out those words that are impossible to be present in keyword list
		$line=filter_word($line);
		# split into an array of words
		my @words=split(/\s+/,$line); 
		if ($#words>=0) {
			$total_funlines++;
		} else {
			next;
		}
		my %present_word;
		for (my $i=0;$i<=$#words;$i++) {
			next if ($words[$i] eq '#');
			print "words[$i] = $words[$i]\n";
			$present_word{$words[$i]}=1;
			my $nlimit=($n_words>$i+1)?($i+1):$n_words;
			for (my $n=2;$n<=$nlimit;$n++) {
				my $pw=''; # previous words;
				my $jlimit=($i>=$n-1)?($i-$n+1):0;
				for (my $j=$i-1;$j>=$jlimit;$j--) { # only consider phrases that contain at most n words
					last if ($words[$j] eq '#');
					print "     words[$j] = $words[$j]\n";
					$pw="$words[$j] ".$pw;
				}
				$present_word{$pw.$words[$i]}=1;
			}
		}
		foreach (keys %present_word) {
		    print "present_word key: $_\n";
			$freq{$_}++;
		}
	}
	my %freq_keyword;
	foreach my $w (keys %freq) {
		next unless (word_satisfy($w));
		$freq{$w}=$freq{$w}/$total_funlines;
		next unless ($freq{$w}>$freq_cutoff);
		push(@{$freq_keyword{sprintf("%.2f",$freq{$w})}},$w);
	}

	my %return_freq;
	my $return_n_words_count=0;

	NEXTWORD:foreach my $f (sort {$b<=>$a} keys %freq_keyword) {
		if (scalar(@{$freq_keyword{$f}})==1) {
			$return_n_words_count+=scalar(my @tmp=split(" ",$freq_keyword{$f}->[0]));
			last NEXTWORD if ($return_n_words_count>$return_n_words);
			$return_freq{$freq_keyword{$f}->[0]}=$f;
		} else {
			my $ref=$freq_keyword{$f};
			my %noncontained;
			WORD:foreach my $wa (@$ref) {
				foreach my $wb (@$ref) {
					next if ($wa eq $wb);
					if (index($wb,$wa)>=0) {
						next WORD; # $wb contain $wa
					}
				}
				$noncontained{$wa}=1;
			}
			if (scalar(keys %noncontained)) {
				foreach my $w (keys %noncontained) {
					$return_n_words_count+=scalar(my @tmp=split(" ",$w));
					last NEXTWORD if ($return_n_words_count>$return_n_words);
					$return_freq{$w}=$f;
				}
			} else { # didn't find the one which is non-contained, then find the one with most words
				my ($mostwords) = sort {scalar(my @tmpb=split(" ",$b)) <=> scalar(my @tmpa=split(" ",$a))} @$ref;
				$return_n_words_count+=scalar(my @tmp=split(" ",$mostwords));
				last NEXTWORD if ($return_n_words_count>$return_n_words);
				$return_freq{$mostwords.' LONGEST'}=$f;
			}
		}
	}

	return \%return_freq;
}

sub punctuation_mark {
	my $l=$_[0];
	$l=~s/\, / \# /g;
	$l=~s/\; / \# /g;
	$l=~s/\. / \# /g;
	$l=~s/\.$/ \# /g;
	$l=~s/\:/ \# /g;
	$l=~s/\[|\]|\{|\}/ \# /g;

	while ($l=~/ \([^\)]*\)[^\-]/) { #only starting with " (", then the next ")" can be replaced
	    print "in while loop, punctuation_mark(): $l\n";
		$l=~s/ \(([^\)]*)\)[^\-]/ \# $1 \# /g;
	}
	print "return value, punctuation_mark(): $l\n";
	return $l;
}

sub linecase {
	my $l=$_[0];
	my @w=split(/\s+/,$l); 
	for (my $i=0;$i<=$#w;$i++) {
		$w[$i]=case($w[$i]);
	}
	return join(" ",@w);

}

sub filter_word {
	my $line=$_[0];
	foreach my $w (@noword) {
		if ($w=~/^\-/) {
		    print "in filter_word() noword: $w\n";
		    print "before                   $line\n";
			$line=~s/$w/ \# /g;
		    print "after                    $line\n";
		} else {
		    print "in filter_word() !noword: $w\n";
		    print "before                    $line\n";
			$line=~s/[^a-zA-Z]$w[^a-zA-Z]/ \# /g;
		    print "step 1                    $line\n";
			$line=~s/^$w[^a-zA-Z]/ \# /g;
		    print "step 2                    $line\n";
			$line=~s/[^a-zA-Z]$w$/ \# /g;
		    print "step 3                     $line\n";
		}
	}
	foreach my $w (@dashword) {
		if ($line=~/\-$w/) {
			$line=~s/\-$w/ $w/g;
		}
		print "in foreach dashword, filter_word(): $line\n";
	}
	return $line;
}

sub case {
	my $w=$_[0];
	if ($w=~/^[A-Z][a-z]/) {
		if (defined $word_filter{capitalword}->{$w}) { # capitalword is defined
		    print "in case, capitalword defined [A-Z][a-z]: $w\n";
			return $w;
		} else {
		    print "in case, capitalword !defined [A-Z][a-z]: $w\n";
			foreach my $cw (%{$word_filter{capitalword}}) { # capitalword as the start of certain words
				if ($w=~/^$cw/) {
					return $w;
				}
			}
			return lcfirst($w);
		}
	} elsif ($w=~/^[A-Z][A-Z]/) {
		if (defined $word_filter{capitalword}->{$w}) { # capitalword is defined
		    print "in case, capitalword defined [A-Z][A-Z]: $w\n";
			return $w;
		} else {
		    print "in case, capitalword defined [A-Z][A-Z]: $w\n";
			foreach my $cw (%{$word_filter{capitalword}}) { # capitalword as the start of certain words
				if ($w=~/^$cw/) {
					return $w;
				}
			}
			return lc($w);
		}
	} else {
	    print "in case, [a-z][a-z]: $w\n";
		return $w;
	}
}

sub word_satisfy {
	my $w=$_[0];
	# return 1 if satisfied as a single keyword
	# return 0 if not
	if (defined $word_filter{nosingleword}->{$w}) { # if listed as nosingleword, not satisfied
		return 0;
	} elsif (length($w)==1) { # if only 1 character, not satisfied
		return 0;
	} elsif ($w!~/[a-zA-Z]/) { # if don't have character, not satisfied
		return 0;
	} elsif ($w=~/pf\d{5}/) { # pfam accession
		return 0;
	} elsif ($w=~/sp\|/) { # swissprot accession
		return 0;
	} else {
		return 1;
	}
}

1;
