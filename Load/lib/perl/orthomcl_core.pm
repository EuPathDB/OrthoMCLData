package orthomcl_core;
use strict;
use orthomcl_io;

require Exporter;

our @ISA= qw (Exporter);
our @EXPORT = qw(
              construct_graph_pairwise
              rbh_to_file_write
              inparalog_weight_normalization
              construct_graph_all
);

# this module contains 4 core subroutines used in orthomcl.pl
# construct_graph_pairwise
# rbh_to_file_write
# inparalog_weight_normalization
# construct_graph_all

# and 7 other subroutines:
# make_inparalog
# make_ortholog
# rbh_weight
# satisfy_cutoff
# blastquery_ab
# non_redundant_list
# calc_weight


sub construct_graph_pairwise {
	my $bpo_idx_ref;
	if ($::setting{'BPO_IDX_MODE'} eq 'all') {
		$bpo_idx_ref = bpo_idx_file('read','#all#');
	}
	foreach my $taxon (@::taxa) {
		my $ltc_flag=ltc_file('test',$taxon,$taxon);
		if ($ltc_flag==1) {#this comparison is already complete
			write_log("\nSkipping in-paralog identification for $taxon\n");
			next;
		} elsif ($ltc_flag==0) {#this comparison is incomplete yet
			ltc_file('clear',$taxon,$taxon);
		}
		ltc_file('lock',$taxon,$taxon);
		write_log("\nIdentifying in-paralog pairs from $taxon\n");
		if ($::setting{'BPO_IDX_MODE'} eq 'taxon') {
			$bpo_idx_ref = bpo_idx_file('read',$taxon);
		}
		my ($mtx_ref,$sumw,$c) = make_inparalog($bpo_idx_ref,$taxon);# identification of inparalogs
		mtx_file('write',$taxon,$taxon,$mtx_ref);
		ltc_file('unlock',$taxon,$taxon);
	}
	
	if (exists($::setting{'PIPELINE_STOP'}) && $::setting{'PIPELINE_STOP'}==1) {
		write_log("\nOrthoMCL pipeline stopped after in-paralog identifications, according to user's request (PIPELINE_STOP=$::setting{'PIPELINE_STOP'})\nPlease restart to continue the parallel analysis of (co-)ortholog identifications by setting PIPELINE_STOP to 2 and setting FORMER_RUN_SUBDIR to $::setting{'RUN_SUBDIR'}.\n");
		die;
	}

	for(my $i=0;$i<$#::taxa;$i++) {
		for(my $j=$i+1;$j<=$#::taxa;$j++) {
			my $ltc_flag=ltc_file('test',$::taxa[$i],$::taxa[$j]);
			if ($ltc_flag==1) {#this comparison is already complete
				write_log("\nSkipping ortholog identification between $::taxa[$i] and $::taxa[$j]\n");
				next;
			} elsif ($ltc_flag==0) {#this comparison is incomplete yet
				ltc_file('clear',$::taxa[$i],$::taxa[$j]);
			}
			ltc_file('lock',$::taxa[$i],$::taxa[$j]);
			write_log("\nIdentifying ortholog pairs between $::taxa[$i] and $::taxa[$j]\n");
			if ($::setting{'BPO_IDX_MODE'} eq 'taxon') {
				%{$bpo_idx_ref}=();
				my $bpo_idx_ref_i = bpo_idx_file('read',$::taxa[$i]);
				my $bpo_idx_ref_j = bpo_idx_file('read',$::taxa[$j]);
				foreach my $g (keys %$bpo_idx_ref_i) {
					$bpo_idx_ref->{$g}=$bpo_idx_ref_i->{$g};
				}
				foreach my $g (keys %$bpo_idx_ref_j) {
					$bpo_idx_ref->{$g}=$bpo_idx_ref_j->{$g};
				}
				undef $bpo_idx_ref_i; undef $bpo_idx_ref_j;
			}
			my ($mtx_ref,$sumw,$c_ortholog) = make_ortholog($bpo_idx_ref,$::taxa[$i],$::taxa[$j]); # identification of orthologs
			write_log("Appending co-ortholog pairs between $::taxa[$i] and $::taxa[$j]: ");  
			my $c_coortholog=0;
			my %mtx_rbh;
			foreach my $pi (keys %$mtx_ref) {%{$mtx_rbh{$pi}}=%{$mtx_ref->{$pi}};}  #make a copy of current %mtx into %mtx_rbh	for(my $i=0;$i<$#::taxa;$i++) {


			my %p1=%{mtx_file('read',$::taxa[$i],$::taxa[$i])};
			my %p2=%{mtx_file('read',$::taxa[$j],$::taxa[$j])};

			my %para;
			foreach my $p (keys %p1) {$para{$p}=$p1{$p};}
			foreach my $p (keys %p2) {$para{$p}=$p2{$p};}
			undef %p1; undef %p2;

			foreach my $n (keys %mtx_rbh) {
				my (@nodes_1, @nodes_2);

				if (exists($para{$n})) {push (@nodes_1, $n, keys %{$para{$n}});}
				else {push (@nodes_1, $n);}

				foreach (keys %{$mtx_rbh{$n}}) {
					if (exists($para{$_})) {push (@nodes_2, $_, keys %{$para{$_}});}
					else {push (@nodes_2, $_);}
				}

				@nodes_1=@{nonredundant_list(\@nodes_1)};
				@nodes_2=@{nonredundant_list(\@nodes_2)};

				foreach my $node_1 (@nodes_1) {
					foreach my $node_2 (@nodes_2) {
						next if(exists($mtx_ref->{$node_1}) && exists($mtx_ref->{$node_1}->{$node_2}));#if edge is already established for $node_1 and $node_2, skip
						my ($pv1,$flag1)=blastquery_ab($bpo_idx_ref,$node_1,$node_2);
						my ($pv2,$flag2)=blastquery_ab($bpo_idx_ref,$node_2,$node_1);;
						if (($flag1==1) && ($flag2==1)) {
							my $w1=calc_weight($pv1);
							my $w2=calc_weight($pv2);
							my $w = ($w1+$w2)/2; # use averaged score as edge weight
							$mtx_ref->{$node_1}->{$node_2}=[sprintf("%.3f",$w),'c1',$pv1,$pv2];#c1:co-ortholog edges(putative);1 is a redundancy flag
							$mtx_ref->{$node_2}->{$node_1}=[sprintf("%.3f",$w),'c2',$pv2,$pv1];#c2:co-ortholog edges(putative);2 is a redundancy flag
							$sumw += $w;
							$c_coortholog++;
						}
					}
				}
			}
			write_log("$c_coortholog pairs\n");
			my $avgw = 'N/A';
			if ($c_ortholog+$c_coortholog) {
				$avgw = $sumw/($c_ortholog+$c_coortholog);
			}
			#(co-)ortholog weight normalization
			write_log("$::taxa[$i] and $::taxa[$j] average weight: $avgw\n");
			foreach my $node_1 (keys %$mtx_ref) {
				foreach my $node_2 (keys %{$mtx_ref->{$node_1}}) {
					$mtx_ref->{$node_1}->{$node_2}->[0] = sprintf("%.3f", $mtx_ref->{$node_1}->{$node_2}->[0]/$avgw);
				}
			}

			mtx_file('write',$::taxa[$i],$::taxa[$j],$mtx_ref);
			ltc_file('unlock',$::taxa[$i],$::taxa[$j]);
		}
	}

	undef $bpo_idx_ref;
	%::gindex=();

	if (exists($::setting{'PIPELINE_STOP'}) && $::setting{'PIPELINE_STOP'}==2) {
		write_log("\nOrthoMCL pipeline stopped after (co-)ortholog identifications, according to user's request (PIPELINE_STOP=$::setting{'PIPELINE_STOP'})\nPlease restart OrthoMCL analysis by commenting out PIPELINE_STOP line and setting FORMER_RUN_SUBDIR to $::setting{'RUN_SUBDIR'}.\n");
		exit(0);
	}

}

sub rbh_to_file_write {
#please note that mtx files are redundant, A-B and B-A both are present
#but rbh files are not, only A-B is present

	if (-e $::setting{'RBH_FILE'}) {
		write_log("\nRBH file ($::setting{'RBH_FILE'}) is present, skipping writing RBH data\n");
		return;
	}

	open (RBH,">$::setting{'RBH_FILE'}") or die "can't create $::setting{'RBH_FILE'}";
	$::filehandle{'RBH'}=*RBH;


	write_log("Writing RBH (including Reciprocal Better Hits within species and Reciprocal Best Hits across species) data to file...\n");
	for(my $i=0;$i<=$#::taxa;$i++) {
		write_rbh("#$::taxa[$i]-$::taxa[$i]\n");
		my %mtx=%{mtx_file('read',$::taxa[$i],$::taxa[$i])};
		foreach my $node_1 (keys %mtx) {
			foreach my $node_2 (keys %{$mtx{$node_1}}) {
				if ($mtx{$node_1}->{$node_2}->[1]=~/i1/) {
					write_rbh("$node_1	$node_2	i	".$mtx{$node_1}->{$node_2}->[2]."	".$mtx{$node_1}->{$node_2}->[3]."\n");
				}
			}
		}
	}	
			
	for(my $i=0;$i<$#::taxa;$i++) {
		for(my $j=$i+1;$j<=$#::taxa;$j++) {
			write_rbh("#$::taxa[$i]-$::taxa[$j]\n");
			my %mtx=%{mtx_file('read',$::taxa[$i],$::taxa[$j])};
			foreach my $node_1 (keys %mtx) {
				foreach my $node_2 (keys %{$mtx{$node_1}}) {
					if ($mtx{$node_1}->{$node_2}->[1]=~/o1/) {
						write_rbh("$node_1	$node_2	o	".$mtx{$node_1}->{$node_2}->[2]."	".$mtx{$node_1}->{$node_2}->[3]."\n");
					}
				}
			}
		}
	}
	close(RBH);
}

sub inparalog_weight_normalization {

	my %ortho_node;#nodes having putative orthologs

	for(my $i=0;$i<$#::taxa;$i++) {
		for(my $j=$i+1;$j<=$#::taxa;$j++) {
			my %mtx=%{mtx_file('read',$::taxa[$i],$::taxa[$j])};
			NODE1:foreach my $node_1 (keys %mtx) {
				foreach my $node_2 (keys %{$mtx{$node_1}}) {
					if ($mtx{$node_1}->{$node_2}->[1]=~/o/) {
						$ortho_node{$node_1}=1;
						next NODE1;
					}
				}
			}
		}
	}

	foreach my $taxon (@::taxa) {
		write_log("\ncalculate average weight from $taxon\n");
		my %mtx=%{mtx_file('read',$taxon,$taxon)};

		my $count=0; my $sum=0;
		my $count_all=0; my $sum_all = 0;

		foreach my $node_1 (keys %mtx) {
			foreach my $node_2 (keys %{$mtx{$node_1}}) {
				$count_all++; $sum_all += $mtx{$node_1}->{$node_2}->[0];
				if (exists($ortho_node{$node_1}) || exists($ortho_node{$node_2})) {
					$count++;
					$sum += $mtx{$node_1}->{$node_2}->[0];
				}
			}
		}

		my $avgw = 0;
		
		# normalize the in-paralog weights by the average weight of inparalogs which have orthologs in other species
		# common case, for eukaryotes and most prokaryotes
		if ($count) {
			$avgw = $sum/$count;
		}
		# OR normalize the in-paralog weights by the average weight
		# not common
		elsif ($count_all) {
			$avgw = $sum_all/$count_all;
			write_log("taxon average weight is calculated based on all inparalog pairs\n");
		}
		# OR no normalization since $count_all=0 and there is nothing stored in %weight
		# not common, useful for prokaryotes or pathogens 

		write_log("$taxon average weight: $avgw\n");
		foreach my $node_1 (keys %mtx) {
			foreach my $node_2 (keys %{$mtx{$node_1}}) {
				$mtx{$node_1}->{$node_2}->[0] = sprintf("%.3f", $mtx{$node_1}->{$node_2}->[0]/$avgw);
			}
		}
		mtx_file('write',$taxon,$taxon,\%mtx);
	}
	inparalog_weight_normalized('lock');
}

sub construct_graph_all {
	write_log("\nConverting OrthoMCL graph into MCL matrix...\n");
	
	# our approach is to write out smaller files, one per taxon, to optimize on 
	# memory usage. Each mtx file has hits both ways: a->b as well as b->a. This means
	# that we need to pick only the ids from the current taxon of interest. We achieve
	# this by reading in the GG file, and using a result only if the id is present in 
	# list of ids for a given taxon.

	my (%taxon_idlist, $line);
	open (GGFILE, "< $::setting{GG_FILE}") 
		or die "Unable to open GG file: $!\n";
	while ($line = <GGFILE>) {
		$line =~ s/\s+$//;
		my ($taxon, $idlist) = split(/:/, $line);
		$taxon_idlist{$taxon} = " " . $idlist . " ";
	}

	print localtime(time()) . " (construct_graph_all) Finished reading GG file";

	close GGFILE;

	my %mtx_local;
	my $mtxsize = 0;
	my $mtxlocal_prefix = $::setting{'MTX_DIR'} . "/mcl_input_";

	foreach my $a (0 .. $#::taxa) {
		my $mtxlocal_filename = $mtxlocal_prefix . $::taxa[$a] . ".part";
		my $idlist_line = $taxon_idlist{$::taxa[$a]};
		if (-e $mtxlocal_filename) {
			if (open (EXISTINGMTX, "< $mtxlocal_filename")) {
				#count lines to add to the mtxsize
				my ($line, $tempcount);
				while ($line = <EXISTINGMTX>) {
					$tempcount++;
				}
				close EXISTINGMTX;
				$mtxsize += $tempcount;
				print "$mtxlocal_filename exists. Skipping...\n";
				next;
			} else {
				warn "$mtxlocal_filename exists, but unable to open: $! . Redoing...\n";
			}
		}
		
		foreach my $b (0 .. $#::taxa) {
			#Feng says we shouldn't skip!
			#($a == $b) and next;

			my %mtx;
			my $mtxlocal_filename;

			print "About to read $::taxa[$a] and $::taxa[$b] \n";
			
			if ($a < $b) {
				%mtx = %{mtx_file('read', $::taxa[$a], $::taxa[$b])};
			} else {
				%mtx = %{mtx_file('read', $::taxa[$b], $::taxa[$a])};
			}

			foreach my $protein_a (keys %mtx) {
				($idlist_line =~ / $protein_a /) or next;
				foreach my $protein_b (keys %{$mtx{$protein_a}}) {
					$mtx_local{$protein_a}->{$protein_b} = $mtx{$protein_a}->{$protein_b}
				}
			}
			print "Finished $::taxa[$a] and $::taxa[$b] \n";
		}
		
		write_mtxlocal_index(\%mtx_local, $mtxlocal_filename);
		$mtxsize += scalar (keys %mtx_local);
		undef %mtx_local;
	}
	
	my $timestamp = "[" . localtime(time()) . "]";
	print "$timestamp Finished generating MCL input files per taxon. Matrix size: $mtxsize\n";
	write_log("$timestamp Finished generating MCL input files per taxon. Matrix size: $mtxsize\n");
	
	print "$::setting{MTX_DIR} \n";
	opendir (MTXDIRHANDLE, $::setting{'MTX_DIR'}) 
		or die "Unable to open MTX_DIR (FORMER_RUN_SUBDIR/mtx): $!\n";
	
	print "Now combining the MCL files into a single MCL input file...\n";

	my @mtxlocal_files = grep {/^mcl_input_/} readdir MTXDIRHANDLE;
	closedir MTXDIRHANDLE;

	if (scalar (@mtxlocal_files) != scalar (@::taxa)) {
		die "The number of mtx local files (" . scalar (@mtxlocal_files) 
						. ") does not match the taxa number (" . (scalar (@::taxa))
						. "). Exiting...\n";
	}
    open (MTX,"> $::setting{'MTX_FILE'}")
		or die "cannot write to file $::setting{MTX_FILE}: $!\n";
    print MTX "(mclheader\nmcltype matrix\ndimensions ".$mtxsize."x".$mtxsize."\n)\n\n(mclmatrix\nbegin\n\n";
	
	foreach my $mtxlocal_file (@mtxlocal_files) {
		my $fullname = $::setting{'MTX_DIR'} . "/" . $mtxlocal_file;
		open (MTXLOCAL, "< $fullname")
			or die "Unable to open MTX local file ($fullname): $!\n";
		while ($line = <MTXLOCAL>) {
			print MTX "$line";
		} 
		
		close MTXLOCAL;
	}

	print MTX ")\n\n";
	close MTX;

	print "Finished generating the MCL input file.\n";

}


sub write_mtxlocal_index {
	my ($mtx_local_ref, $mtxlocal_filename) = @_;
	my %mtx_local = %{$mtx_local_ref};
    my $size = scalar(keys %mtx_local);

    print ("\nThere are $size sequences to cluster\n");
    open (MTX,">$mtxlocal_filename") 
		or die "cannot write to file $mtxlocal_filename: $!\n";

    foreach my $node_1 (keys %mtx_local) {
        print MTX $node_1 . "\t";
        foreach my $node_2 (keys %{$mtx_local{$node_1}}) {
            print MTX $node_2.":".$mtx_local{$node_1}->{$node_2}->[0]." ";
        }
        print MTX "\$\n";
    }
    
    close (MTX);
	
	my $timestamp = "[" . localtime(time()) . "]";
    write_log("$timestamp Matrix ($size) file $mtxlocal_filename generated\n");
    print "$timestamp Matrix ($size) file $mtxlocal_filename generated\n";
}

sub make_inparalog {
	my ($bpo_idx_ref,$taxon_id) = @_;
	my %sbh;#single-way/one-way better hit
	foreach my $query_id (@{$::gindex{$taxon_id}}) {
		my ($offset_s,$offset_e);
		if (exists($bpo_idx_ref->{$query_id})) {
			($offset_s,$offset_e)=split(";",$bpo_idx_ref->{$query_id});
		} else {next;}
		seek($::filehandle{'BPO'},$offset_s,0);
		my $pv_cut=10;
		LOOP:while (my $line=readline($::filehandle{'BPO'})) {
			$line=~s/\r|\n//g;
			my ($pv,$subject_id,$flag)=satisfy_cutoff($line);
			if ((tell($::filehandle{'BPO'})>$offset_e) || ($pv>$pv_cut)) {last LOOP;}
			next unless (($flag==1) && ($query_id ne $subject_id));
			if (not exists($::gindex2{$subject_id})) {
				write_log("$subject_id not defined in GG file:\n$line\n");
				next LOOP;
			}
			if ($::gindex2{$subject_id} ne $taxon_id) {$pv_cut=$pv; next LOOP;}
			$sbh{$query_id}->{$subject_id}=$pv;
		}
	}
	my $no_tmp = scalar(keys %sbh);
	write_log("$no_tmp sequences have one-way better hits within species\n");
	return rbh_weight(\%sbh, 'i');#i:in-paralog edges(putative)
}

sub make_ortholog {
	my ($bpo_idx_ref,$ta,$tb) = @_;
	my %sbh;#single-way/one-way best hit
	foreach my $query_id (@{$::gindex{$ta}}) {
		my ($offset_s,$offset_e);
		if (exists($bpo_idx_ref->{$query_id})) {
			($offset_s,$offset_e)=split(";",$bpo_idx_ref->{$query_id});
		} else {next;}
		seek($::filehandle{'BPO'},$offset_s,0);
		my $pv_cut=10;
		LOOP:while (my $line=readline($::filehandle{'BPO'})) {
			$line=~s/\r|\n//g;
			my ($pv,$subject_id,$flag)=satisfy_cutoff($line);
			if ((tell($::filehandle{'BPO'})>$offset_e) || ($pv>$pv_cut)) {last LOOP;}
			if (not exists($::gindex2{$subject_id})) {
				write_log("$subject_id not defined in GG file:\n$line\n");
				next LOOP;
			}
			next unless (($flag==1) && ($::gindex2{$subject_id} eq $tb));
			$sbh{$query_id}->{$subject_id}=$pv;
			$pv_cut=$pv;
		}
	}
	my $no_tmpa=scalar(keys %sbh);
	foreach my $query_id (@{$::gindex{$tb}}) {
		my ($offset_s,$offset_e);
		if (exists($bpo_idx_ref->{$query_id})) {
			($offset_s,$offset_e)=split(";",$bpo_idx_ref->{$query_id});
		} else {next;}
		seek($::filehandle{'BPO'},$offset_s,0);
		my $pv_cut=10;
		LOOP:while (my $line=readline($::filehandle{'BPO'})) {
			$line=~s/\r|\n//g;
			my ($pv,$subject_id,$flag)=satisfy_cutoff($line);
			if ((tell($::filehandle{'BPO'})>$offset_e) || ($pv>$pv_cut)) {last LOOP;}
			if (not exists($::gindex2{$subject_id})) {
				write_log("$subject_id not defined in GG file:\n$line\n");
				next LOOP;
			}
			next unless (($flag==1) && ($::gindex2{$subject_id} eq $ta));
			$sbh{$query_id}->{$subject_id}=$pv;
			$pv_cut=$pv;
		}
	}
	my $no_tmpb=scalar(keys %sbh)-$no_tmpa;

	write_log("$no_tmpa($ta)/$no_tmpb($tb) sequences have one-way best hits from the other species\n");
	return rbh_weight(\%sbh, 'o');#o:ortholog edges(putative)
}

sub rbh_weight {
	my %sbh       = %{$_[0]};
	my $edge_flag = $_[1];

	my %mtx;
	my $count=0;
	my $sumw=0;

	foreach my $query_id (sort keys %sbh) {
		foreach my $subject_id (keys %{$sbh{$query_id}}) { # all the subject_id is oneway best hit of query_id
			next if (exists($mtx{$query_id}) && exists($mtx{$query_id}->{$subject_id}));# this pair was already identified as RBH pair
			if (exists($sbh{$subject_id}) && exists($sbh{$subject_id}->{$query_id})) { # means query_id is also oneway best hit of subject_id, so RBH pair
				my $w1=calc_weight($sbh{$query_id}->{$subject_id});
				my $w2=calc_weight($sbh{$subject_id}->{$query_id});
				my $w = ($w1+$w2)/2;
				$sumw += $w;
				$count++;
				# use averaged score as edge weight
				$mtx{$query_id}->{$subject_id}=[sprintf("%.3f",$w),$edge_flag.'1',$sbh{$query_id}->{$subject_id},$sbh{$subject_id}->{$query_id}];
				$mtx{$subject_id}->{$query_id}=[sprintf("%.3f",$w),$edge_flag.'2',$sbh{$subject_id}->{$query_id},$sbh{$query_id}->{$subject_id}];
			}
		}
	}
	write_log("$count sequence pairs were identified as Reciprocal Better/Best Hit\n");
	return (\%mtx, $sumw, $count);
}

sub satisfy_cutoff {
#1;At1g01190;535;At1g01190;535;0.0;97;1:1-535:1-535.
#0 1         2   3         4   5   6  7
#	my ($subject_id,$pv,$pi,$hsp_info,$query_len,$subject_len)=(split(";",$_[0]))[3,5,6,7,2,4];
	my ($subject_id,$pv,$pi,$pm,$query_len,$subject_len)=(split(";",$_[0]))[3,5,6,7,2,4];
	my $flag=1;   # 1, satisfy cutoff; 0, otherwise.
	
	if (exists($::setting{'PVALUE_CUTOFF'})) {
		if($pv > $::setting{'PVALUE_CUTOFF'}) {
			$flag=0;
			return ($pv,$subject_id,$flag);
		}
	}
	if (exists($::setting{'PIDENT_CUTOFF'})) {
		if ($pi < $::setting{'PIDENT_CUTOFF'}) {
			$flag=0;
			return ($pv,$subject_id,$flag);
		}
	}
	if (exists($::setting{'PMATCH_CUTOFF'})) {
#		if (calc_pmatch($query_len,$subject_len,$hsp_info) < $::setting{'PMATCH_CUTOFF'}) {
		if ($pm < $::setting{'PMATCH_CUTOFF'}) {
			$flag=0;
			return ($pv,$subject_id,$flag);
		}
	}
	return ($pv,$subject_id,$flag);
}


sub blastquery_ab {
	my ($bpo_idx_ref,$a,$b) = @_;

	my ($offset_s,$offset_e);
	if (exists($bpo_idx_ref->{$a})) {
		($offset_s,$offset_e)=split(";",$bpo_idx_ref->{$a});
	} else {return (undef,0);}
	seek($::filehandle{'BPO'},$offset_s,0);
	while (my $line=readline($::filehandle{'BPO'})) {
		$line=~s/\r|\n//g;
		my ($pv,$subject_id,$flag)=satisfy_cutoff($line);
		if (tell($::filehandle{'BPO'})>$offset_e) {return (undef,0);}
		next unless (($flag==1) && ($subject_id eq $b));
		return ($pv,$flag);
	}
	return (undef,0);
}

sub nonredundant_list {
	my $list_ref=$_[0];
	my %nr;
	foreach (@{$list_ref}) {$nr{$_}=1;}
	my @nr=sort (keys %nr);
	return \@nr;
}

sub calc_weight {
	#use -log10(P) as weights and treat P=0 as -log10(P)=$::setting{'MAX_WEIGHT'}
	my $pvalue=$_[0];
	if($pvalue == 0) {
		return $::setting{'MAX_WEIGHT'};
	} else {
		return -log($pvalue)/log(10);
	}
}

sub calc_pmatch {
	my ($q_len,$s_len,$hsp_info) = @_;
	my (%s_start, %s_length, %q_start, %q_length);
	my @hsp=split(/\./,$hsp_info);
	foreach (@hsp) {
		if (/(\d+)\:(\d+)\-(\d+)\:(\d+)\-(\d+)/) {
			$s_start{$1}=$4; 
			$s_length{$1}=$5-$4+1;
			$q_start{$1}=$2;
			$q_length{$1}=$3-$2+1;
		}
	}
	my $s_matchlen = calc_matchlen(\%s_start,\%s_length);
	my $q_matchlen = calc_matchlen(\%q_start,\%q_length);
	if ($s_len >= $q_len) {
		return 100*$q_matchlen/$q_len;
	}else{
		return 100*$s_matchlen/$s_len;
	}
}

sub calc_matchlen {
	my %start        = %{$_[0]}; 
	my %length       = %{$_[1]};
	my @starts = sort{$start{$a}<=>$start{$b}} (keys %start);
	return $length{$starts[0]} if(scalar(@starts)==1);
	my $i=1; 
	my $pos =  $start{$starts[0]} + $length{$starts[0]};
	my $match_length = $length{$starts[0]}; 
	while ($i<scalar(@starts)) {
		if ($length{$starts[$i]} + $start{$starts[$i]} <= $pos) {
			$i++;
			next;
		}
		if ($start{$starts[$i]} > $pos) {
			$match_length += $length{$starts[$i]};
			$pos = $start{$starts[$i]} + $length{$starts[$i]};
		} else {
			$match_length += $length{$starts[$i]} - ($pos - $start{$starts[$i]});
			$pos = $start{$starts[$i]} + $length{$starts[$i]};
		}
		$i++;
	}

	return $match_length;
}

1;
