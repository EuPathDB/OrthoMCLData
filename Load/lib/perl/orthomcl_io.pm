package orthomcl_io;
use strict;
use Storable;

require Exporter;

our @ISA= qw (Exporter);
# this module contains 15 io/file subroutines used in orthomcl.pl
our @EXPORT = qw(
                  read_setting
                  write_setting
                  read_ggfile
                  setup_run
                  setup_bpo_idx
                  confirm_graph_completeness
                  inparalog_weight_normalized
                  index_bpo
                  bpo_idx_file
                  mtx_file
                  ltc_file
                  write_log
                  write_rbh
                  write_matrix_index
                  execute_MCL
                  mcl_backindex
);


sub read_setting {
	my $setting_file=$_[0];
	open(SETTING,$setting_file) || die "can't open $setting_file";
	while (<SETTING>) {
		next if (/\#/);
		$_=~s/\r|\n//g;
		next if ($_ eq '');
		my ($k,$v)=split("=",$_);
		$::setting{$k}=$v;
	}
	close(SETTING);
	if (not exists($::setting{'FORMER_RUN_SUBDIR'})) {
		my @required_set = qw(MAX_WEIGHT DIR BPO_FILE BPO_IDX_MODE GG_FILE MCL INFLATION);
		foreach (@required_set) {
			die "$_ needs to be defined in $setting_file" unless (exists($::setting{$_}));
		}
		if (($::setting{'BPO_IDX_MODE'} ne 'all') && ($::setting{'BPO_IDX_MODE'} ne 'taxon')) {
			die "BPO_IDX_MODE (all/taxon) is set to a disallowed value: ".$::setting{'BPO_IDX_MODE'};
		}
	} else {
		foreach (keys %::setting) {
			if (($_ eq 'PVALUE_CUTOFF') || ($_ eq 'PIDENT_CUTOFF') || ($_ eq 'PMATCH_CUTOFF')) {
				delete $::setting{$_}; #all the other settings are expired, need to be deleted
			}
		}
	}
}


sub write_setting {

	my @o=qw(
		-- DIR GG_FILE BPO_FILE
                -- RUN_DIR FORMER_RUN_SUBDIR FORMER_RUN_DIR FORMER_GRAPH_COMPLETE TAXA_LIST
		-- OUT_FILE RBH_FILE
		-- PVALUE_CUTOFF PIDENT_CUTOFF PMATCH_CUTOFF MAX_WEIGHT
		-- MCL INFLATION
		-- LOG_FILE SET_FILE
		-- START_TIME END_TIME
		-- MTX_DIR MCL_DIR MTX_FILE IDX_FILE MCL_FILE
                -- BPO_IDX_MODE BPO_IDX_FILE BPO_IDX_DIR
                -- PIPELINE_STOP --
	);

	open(SETTING,'>'.$::setting{'SET_FILE'}) || die;
	foreach my $p (@o) {
		if ($p=~/\-\-/) {
			print SETTING "-----------------------------------------------\n";
		} elsif (exists($::setting{$p})) {
			print SETTING "$p=$::setting{$p}\n";
		}
	}
	close(SETTING);

}


sub read_ggfile {
	my $flag=shift(@_);
	open (GG,$::setting{'GG_FILE'}) or die "can't open $::setting{'GG_FILE'}!";
	while (<GG>) {
		$_=~s/\r|\n//g;
		if (/(\S+)\(\d+\):(.*)/) {
			my $taxon=$1;
			my @genes=split (" ",$2);
			push (@::taxa,$taxon);
			push (@{$::gindex{$taxon}},@genes);
			foreach (@genes) {$::gindex2{$_}=$taxon;}
		} elsif (/(\S+):(.*)/) {
			my $taxon=$1;
			my @genes=split (" ",$2);
			push (@::taxa,$taxon);
			push (@{$::gindex{$taxon}},@genes);
			foreach (@genes) {$::gindex2{$_}=$taxon;}
		}
	}
	close (GG);
}

sub setup_run {
#Normally each OrthoMCL run starts from setting up a new run directory,
#except when FORMER_RUN_SUBDIR is set but graph construction is not 
#complete in the former run.

#First determine the new run directory
	my $date=(split(" ",$::setting{'START_TIME'}))[1]."_".(split(" ",$::setting{'START_TIME'}))[2];
	$::setting{'RUN_SUBDIR'}=$date;
	$::setting{'RUN_DIR'} = $::setting{'DIR'}.$date;
	if (-e $::setting{'RUN_DIR'}) {
		my $no=2;
		while (-e $::setting{'RUN_DIR'}."_".$no) {
			$no++;
		}
		$::setting{'RUN_SUBDIR'}=$date.'_'.$no;
		$::setting{'RUN_DIR'}.="_$no";
	}

	$::setting{'RUN_DIR'}.="/";

	if (exists($::setting{'FORMER_RUN_SUBDIR'})) {
		$::setting{'FORMER_RUN_DIR'} = $::setting{'DIR'}.$::setting{'FORMER_RUN_SUBDIR'}.'/';
		$::setting{'MTX_DIR'}        = $::setting{'FORMER_RUN_DIR'}."mtx/";
		$::setting{'RBH_FILE'}    = $::setting{'FORMER_RUN_DIR'}.'orthomcl.rbh';
		$::setting{'FORMER_GRAPH_COMPLETE'}=confirm_graph_completeness();
#if graph in the FORMER_RUN_DIR is not complete, skipping creating a new run directory
		unless ($::setting{'FORMER_GRAPH_COMPLETE'}) {
			$::setting{'RUN_SUBDIR'}=$::setting{'FORMER_RUN_SUBDIR'};
			$::setting{'RUN_DIR'}   =$::setting{'FORMER_RUN_DIR'};
			setup_bpo_idx();
			if (-e $::setting{'FORMER_RUN_DIR'}.'orthomcl_'.$date.'.log') {
				my $no=2;
				while (-e $::setting{'FORMER_RUN_DIR'}.'orthomcl_'.$date.'_'.$no.'.log') {
					$no++;
				}
				$::setting{'LOG_FILE'} = $::setting{'FORMER_RUN_DIR'}.'orthomcl_'.$date.'_'.$no.'.log';
			} else {
				$::setting{'LOG_FILE'} = $::setting{'FORMER_RUN_DIR'}.'orthomcl_'.$date.'.log';
			}
			open (LOG,">$::setting{'LOG_FILE'}") or die "can't create $::setting{'LOG_FILE'}";
			$::filehandle{'LOG'}=*LOG;
			write_log("Writing log to $::setting{'LOG_FILE'}\n");
			return;
		}
		system ("mkdir $::setting{'RUN_DIR'}");
	} else {
		setup_bpo_idx();
		system ("mkdir $::setting{'RUN_DIR'}");
		$::setting{'MTX_DIR'}     = $::setting{'RUN_DIR'}."mtx/";
		system ("mkdir $::setting{'MTX_DIR'}");
		$::setting{'RBH_FILE'}    = $::setting{'RUN_DIR'}.'orthomcl.rbh';
	}

	$::setting{'LOG_FILE'}     = $::setting{'RUN_DIR'}.'orthomcl.log';
	open (LOG,">$::setting{'LOG_FILE'}") or die "can't create $::setting{'LOG_FILE'}";
	$::filehandle{'LOG'}=*LOG;

	$::setting{'MCL_DIR'}      = $::setting{'RUN_DIR'}."mcl/";
	system ("mkdir $::setting{'MCL_DIR'}");
	$::setting{'MTX_FILE'}     = $::setting{'MCL_DIR'}.'orthomcl.matrix';
	$::setting{'IDX_FILE'}     = $::setting{'MCL_DIR'}.'orthomcl.index';
	$::setting{'MCL_FILE'}     = $::setting{'MCL_DIR'}.'orthomcl.mclout';
	$::setting{'OUT_FILE'}     = $::setting{'RUN_DIR'}.'all_orthomcl.out';
	$::setting{'PAT_FILE'}     = $::setting{'RUN_DIR'}.'all_orthomcl.pat';
	$::setting{'SET_FILE'}     = $::setting{'RUN_DIR'}.'orthomcl.setting';
}

sub setup_bpo_idx {
	open (BPO,"$::setting{'BPO_FILE'}") or die "can't open $::setting{'BPO_FILE'}";
	$::filehandle{'BPO'}=*BPO;
	if ($::setting{'BPO_IDX_MODE'} eq 'all') {
		if ($::setting{'BPO_FILE'} =~ m/(\S+)[\.\_]bpo$/) {
			$::setting{'BPO_IDX_FILE'} = $1.'_bpo.idx';
		} else {
			$::setting{'BPO_IDX_FILE'} = $::setting{'BPO_FILE'}.'_bpo.idx';
		}
	} elsif ($::setting{'BPO_IDX_MODE'} eq 'taxon') {
		if ($::setting{'BPO_FILE'} =~ m/(\S+)[\.\_]bpo$/) {
			$::setting{'BPO_IDX_DIR'} = $1.'_bpo_idx/';
		} else {
			$::setting{'BPO_IDX_DIR'} = $::setting{'BPO_FILE'}.'_bpo_idx/';
		}
		system("mkdir $::setting{'BPO_IDX_DIR'}") unless (-e $::setting{'BPO_IDX_DIR'});
	}
}

sub confirm_graph_completeness {
	for(my $i=0;$i<=$#::taxa;$i++) {
		for(my $j=$i;$j<=$#::taxa;$j++) {
			if(ltc_file('test',$::taxa[$i],$::taxa[$j])<=0) {
				return 0;
			}
		}
	}
	return 1;
}

sub inparalog_weight_normalized {
	my $mode=$_[0];
	my $lock_file= $::setting{'MTX_DIR'}.'iw_normalized.lock';
	if ($mode eq 'lock') {
		if (-e $lock_file) {
			write_log("WARNING:Lock file ($lock_file) shouldn't be present\n");
		} else {
			system("touch $lock_file");
		}
	} elsif ($mode eq 'test') {#return two values: 1-normalized;0-not normalized
		if (-e $lock_file) {
			return 1;
		} else {
			return 0;
		}
	}
}

sub index_bpo {
	write_log("\nIndexing BPO file...\n");
	my %bpo_idx;
	my $last_queryid='';
	my $offset=tell($::filehandle{'BPO'});
	if ($::setting{'BPO_IDX_MODE'} eq 'all') {
		return if (-e $::setting{'BPO_IDX_FILE'}); # if already generated, no need to generate again
		while (<BPO>) { # after reading a line, the pointer is on the start of the next line
			$_=~s/\r|\n//g;
			my $curr_queryid=(split(";",$_))[1];
			die "$curr_queryid not defined in GG file" unless (exists($::gindex2{$curr_queryid}));
			if ($curr_queryid ne $last_queryid) {
				$bpo_idx{$last_queryid}.=';'.$offset if ($last_queryid ne '');
				$bpo_idx{$curr_queryid}=$offset;
				$last_queryid=$curr_queryid;
			}
			$offset=tell($::filehandle{'BPO'});
		}
		$bpo_idx{$last_queryid}.=';'.$offset;
		bpo_idx_file('write','#all#',\%bpo_idx);
	} elsif ($::setting{'BPO_IDX_MODE'} eq 'taxon') {
		return if (-e $::setting{'BPO_IDX_DIR'}.$::taxa[0].'.idx'); # if already generated, no need to generate again
		while (<BPO>) {
			$_=~s/\r|\n//g;
			my $curr_queryid=(split(";",$_))[1];
			die "$curr_queryid not defined in GG file" unless (exists($::gindex2{$curr_queryid}));
			if ($curr_queryid ne $last_queryid) {
				$bpo_idx{$::gindex2{$last_queryid}}->{$last_queryid}.=';'.$offset if ($last_queryid ne '');
				$bpo_idx{$::gindex2{$curr_queryid}}->{$curr_queryid}=$offset;
				$last_queryid=$curr_queryid;
			}
#			$offset=tell(BPO);
			$offset=tell($::filehandle{'BPO'});
	}
		$bpo_idx{$::gindex2{$last_queryid}}->{$last_queryid}.=';'.$offset;
		foreach my $taxon_id (@::taxa) {
			bpo_idx_file('write',$taxon_id,$bpo_idx{$taxon_id});
		}
	}
#	close(BPO);
}

sub bpo_idx_file {
	my $mode        = shift @_;
	my $taxon_id    = shift @_;
	my $bpo_idx_ref = shift @_;

	if ($mode eq 'write') {
		die '%bpo_idx'." is empty in 'write' mode of subroutine bpo_idx_file!\n" unless (scalar(keys %$bpo_idx_ref)>0);
		if ($taxon_id eq '#all#') {
			store $bpo_idx_ref, $::setting{'BPO_IDX_FILE'};
		} else {
			my $file = $::setting{'BPO_IDX_DIR'}.$taxon_id.'.idx'; # for taxon specific idx files
			store $bpo_idx_ref, $file;
		}
	} elsif ($mode eq 'read') {
		die '%bpo_idx'." is not empty in 'read' mode of subroutine edge_file!\n" if (scalar(keys %$bpo_idx_ref)>0);
		if ($taxon_id eq '#all#') {
			die "$::setting{'BPO_IDX_FILE'} was not generated, and can not be read!" unless (-e $::setting{'BPO_IDX_FILE'});
			return retrieve($::setting{'BPO_IDX_FILE'});
		} else {
			my $file = $::setting{'BPO_IDX_DIR'}.$taxon_id.'.idx'; # for taxon specific idx files
			die "$file was not generated, and can not be read!" unless (-e $file);
			return retrieve($file);
		}
	}
}


sub mtx_file {
	my $mode     = shift @_;
	my $taxon_a  = shift @_;
	my $taxon_b  = shift @_;
	my $mtx_ref = shift @_;  # 'write' mode: 4 parameters; 'read' mode: 3 parameters, and $edge_ref no need to be provided.
	my $mtx_file= $::setting{'MTX_DIR'}.$taxon_a.'_'.$taxon_b.'.mtx';

	if ($mode eq 'write') {
#		die "\%mtx is empty in 'write' mode of subroutine edge_file!\n" unless (scalar(keys %$mtx_ref)>0);
#		for some prokaryotes especially pathogens, there may be no in-paralogs
		open(MTX,">$mtx_file") || die;
		foreach my $node_1 (sort keys %$mtx_ref) {
			print MTX $node_1;
			foreach my $node_2 (sort keys %{$mtx_ref->{$node_1}}) {
				print MTX ' '.$node_2.';'.join(';',@{$mtx_ref->{$node_1}->{$node_2}});				
			}
			print MTX "\n";
		}
		close(MTX);
	} elsif ($mode eq 'read') {
		die "$mtx_file was not generated, and can not be read!" unless (-e $mtx_file);
		open(MTX,"$mtx_file") || die;
		while (<MTX>) {
			$_=~s/\r|\n//g;
			my @nodes=split(" ",$_);
			my $node_1=shift @nodes;
			foreach my $node_2_info (@nodes) {
				my @node_2_columns=split(";",$node_2_info);
				my $node_2=shift @node_2_columns;
				$mtx_ref->{$node_1}->{$node_2}=\@node_2_columns;
			}
		}
		close(MTX);
		return $mtx_ref;
	}
}


sub ltc_file {#lock/test/clear file
	my $mode     = shift @_;
	my $taxon_a  = shift @_;
	my $taxon_b  = shift @_;
	my $lock_file= $::setting{'MTX_DIR'}.$taxon_a.'_'.$taxon_b.'.lock';
	my $mtx_file= $::setting{'MTX_DIR'}.$taxon_a.'_'.$taxon_b.'.mtx';

	if ($mode eq 'lock') {
		if (-e $lock_file) {
			write_log("WARNING:Lock file ($lock_file) shouldn't be present\n");
		} else {
			system("touch $lock_file");
		}
	} elsif ($mode eq 'unlock') {
		if (-e $lock_file) {
			system("rm $lock_file");
		} else {
			write_log("WARNING:Lock file ($lock_file) should be present\n");
		}
	} elsif ($mode eq 'test') {#return three values: 1-complete;0-incomplete;-1-absent
		unless (-e $lock_file) {
			if (-e $mtx_file) {
				return 1;
			} else {
				return -1;
			}
		}
		if (-e $lock_file) {
			return 0;
		}
	} elsif ($mode eq 'clear') {
		if (-e $lock_file) {system("rm $lock_file");}
		if (-e $mtx_file) {system("rm $mtx_file");}
	} else {
		die("WARNING:$mode is set incorrectly\n");
	}
}


sub write_log {
	my $info = $_[0];
	print STDERR $info;
	print {$::filehandle{'LOG'}} $info;
}

sub write_rbh {
	my $info = $_[0];
	print {$::filehandle{'RBH'}} $info;
}


sub write_matrix_index {

	my $size = scalar(keys %::mtx_all);
	write_log("\nThere are $size sequences to cluster\n");
	open (MTX,">$::setting{'MTX_FILE'}") or die "cannot write to file $::setting{'MTX_FILE'}";
	print MTX "(mclheader\nmcltype matrix\ndimensions ".$size."x".$size."\n)\n\n(mclmatrix\nbegin\n\n";

	my $i=0;
	my %mcl_idx2;
	foreach my $node_1 (sort keys %::mtx_all) {
		$mcl_idx2{$node_1}=$i;$::mcl_idx[$i]=$node_1;
		$i++;
	}
	foreach my $node_1 (sort keys %::mtx_all) {
		print MTX $mcl_idx2{$node_1}."    ";
		foreach my $node_2 (sort keys %{$::mtx_all{$node_1}}) {
			print MTX $mcl_idx2{$node_2}.":".$::mtx_all{$node_1}->{$node_2}->[0]." ";
		}
		print MTX "\$\n";
	}
	print MTX ")\n\n";

	close (MTX);

	write_log("Matrix($size X $size) file $::setting{'MTX_FILE'} generated\n");

	open(IDX,">$::setting{'IDX_FILE'}") or die "cannot write to file $::setting{'IDX_FILE'}";
	foreach my $id (sort { $mcl_idx2{$a} <=> $mcl_idx2{$b} } keys %mcl_idx2) {
		print IDX "$mcl_idx2{$id}\t$id\n";
	}
	close(IDX);
	write_log("\nIndex file $::setting{'IDX_FILE'} generated\n");
}

sub execute_MCL {
	write_log("\nRun MCL program\n  $::setting{'MCL'} $::setting{'MTX_FILE'} -I $::setting{'INFLATION'} -o $::setting{'MCL_FILE'}\n");
	system("$::setting{'MCL'} $::setting{'MTX_FILE'} -I $::setting{'INFLATION'} -o $::setting{'MCL_FILE'}");
	if (-e $::setting{'MCL_FILE'}) {
		write_log("\nMCL result $::setting{'MCL_FILE'} generated!\n");}
	else {die "$::setting{'MCL_FILE'} failed to be generated!";}
}


sub mcl_backindex {

	open (MCL,$::setting{'MCL_FILE'}) or die "can't open $::setting{'MCL_FILE'}";
	my @mcl;
	while (<MCL>) {
		$_=~s/\r|\n|\$//g;
		if (/^(\d+)(.*)/) {
			$mcl[$1]=$2;
		} elsif (/^\s+/) {
			$mcl[$#mcl].=$_;
		}
	}
	close (MCL);


	open (OUT,">$::setting{'OUT_FILE'}") or die "can't write to $::setting{'OUT_FILE'}";
	open (PAT,">$::setting{'PAT_FILE'}") or die "can't write to $::setting{'PAT_FILE'}";
	print PAT "ID	No_Genes	No_Taxa";
	foreach (@::taxa) {print PAT "	$_";}
	print PAT "\n";

	foreach my $mcl_cluster_id (0..$#mcl) {
		$mcl[$mcl_cluster_id]=~s/\s+/ /g;
		my @g=split (" ",$mcl[$mcl_cluster_id]);
		my %t;
		foreach (@g) {
			$t{$::gindex2{$::mcl_idx[$_]}}++;
		}
		print PAT "ORTHOMCL".$mcl_cluster_id."	".scalar(@g)."	".scalar(keys %t);
		foreach my $taxon (@::taxa) {
			if (exists($t{$taxon})) {
				print PAT "	$t{$taxon}";
			} else {
				print PAT "	0";
			}
		}
		print PAT "\n";
		print OUT "ORTHOMCL".$mcl_cluster_id."(".scalar(@g)." genes,".scalar(keys %t)." taxa):";
		foreach (@g) {
			print OUT " $::mcl_idx[$_]($::gindex2{$::mcl_idx[$_]})";
		}
		print OUT "\n";
	}
	
	close(OUT);
	close(PAT);

	write_log("\n\nFinal ORTHOMCL Result: $::setting{'OUT_FILE'} generated!!!\n\n");
	
	@::mcl_idx=();
	%::gindex2=();
}


sub edge_file {
	my $mode     = shift @_;
	my $taxon_a  = shift @_;
	my $taxon_b  = shift @_;
	my $edge_ref = shift @_;  # 'write' mode: 4 parameters; 'read' mode: 3 parameters, and $edge_ref no need to be provided.
	my $edge_file= $::setting{'MTX_DIR'}.$taxon_a.'_'.$taxon_b.'.edge';

	if ($mode eq 'write') {
#		die "%edge is empty in 'write' mode of subroutine edge_file!\n" unless (scalar(keys %$edge_ref)>0);
#		for some prokaryotes especially pathogens, in-paralog weight may be empty
		open(EDGE,">$edge_file") || die;
		foreach my $i (keys %$edge_ref) {
			print EDGE $i;
			foreach (@{$edge_ref->{$i}}) {print EDGE " $_";}
			print EDGE "\n";
		}
		close(EDGE);
	} elsif ($mode eq 'read') {
		die "$edge_file was not generated, and can not be read!" unless (-e $edge_file);
		die "%edge is not empty in 'read' mode of subroutine edge_file!\n" if (scalar(keys %$edge_ref)>0);
		open(EDGE,"$edge_file") || die;
		while (<EDGE>) {
			$_=~s/\r|\n//g;
			my @nodes=split(" ",$_);
			my $i=shift @nodes;
			$edge_ref->{$i}=\@nodes;
		}
		close(EDGE);
		return $edge_ref;
	}
}

sub weight_file {
	my $mode       = shift @_;
	my $taxon_a    = shift @_;
	my $taxon_b    = shift @_;
	my $weight_ref = shift @_;  # 'write' mode: 4 parameters; 'read' mode: 3 parameters, and $weight_ref no need to be provided.
	my $weight_file=$::setting{'MTX_DIR'}.$taxon_a.'_'.$taxon_b.'.weight';

	if ($mode eq 'write') {
#		die "%weight is empty in 'write' mode of subroutine weight_file!\n" unless (scalar(keys %$weight_ref)>0);
#		for some prokaryotes especially pathogens, in-paralog weight may be empty
		open(WEIGHT,">$weight_file") || die;
		foreach my $pair (keys %$weight_ref) {
			print WEIGHT "$pair	$weight_ref->{$pair}\n";
		}
		close(WEIGHT);
	} elsif ($mode eq 'read') {
		die "$weight_file was not generated, and can not be read!" unless (-e $weight_file);
		die "%weight is not empty in 'read' mode of subroutine weight_file!\n" if (scalar(keys %$weight_ref)>0);
		open(WEIGHT,"$weight_file") || die;
		while (<WEIGHT>) {
			$_=~s/\r|\n//g;
			my @info=split("	",$_);
			$weight_ref->{$info[0]}=$info[1];
		}
		close(WEIGHT);
		return $weight_ref;
	}
}


1;