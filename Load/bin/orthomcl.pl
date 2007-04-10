#!/usr/bin/perl -w
use strict;
use orthomcl_io;
use orthomcl_core;


our (%setting,%filehandle);

our (@taxa,%gindex,%gindex2,@mcl_idx);
our %mtx_all;

if (!defined $ARGV[0]) {die "SETTING_FILE needs to be given?";}

$setting{'START_TIME'}=`date`;
read_setting($ARGV[0]);

read_ggfile();
setup_run();

write_log("\nThere are ".scalar(@taxa)." genomes, ".scalar(keys %gindex2)." sequences in $::setting{'GG_FILE'}!\n\n");

if (exists($setting{'TAXA_LIST'})) {
	@taxa=split(' ',$setting{'TAXA_LIST'});
	write_log("According to user's request, OrthoMCL analysis will be performed only for ".scalar(@taxa)." genomes: ".join(' ',@taxa).".\n");
}

if (exists($setting{'FORMER_RUN_DIR'})) {
	if ($setting{'FORMER_GRAPH_COMPLETE'}) {
		write_log("OrthoMCL run directory: $setting{'RUN_DIR'}\n\n");
		write_log("OrthoMCL graph construction is skipped (reading from $setting{FORMER_RUN_DIR})\n");
	} else {
		write_log("OrthoMCL run directory: $setting{'FORMER_RUN_DIR'}\n\n");
		write_log("OrthoMCL graph construction is not complete for $setting{'FORMER_RUN_DIR'}, and now is being continued...\n");
		construct_graph_pairwise();#will only run on those comparisons unfinished previously
		write_log("OrthoMCL graph construction is done for $setting{'FORMER_RUN_DIR'}. Please rerun \"perl orthomcl.pl $ARGV[0]\".\n");
		die;
	}
} else {
	write_log("OrthoMCL run directory: $setting{'RUN_DIR'}\n");
	index_bpo();
	write_log("\nOrthoMCL graph construction starts...\n");
	construct_graph_pairwise();
	write_log("\nOrthoMCL graph construction is complete\n");
}

rbh_to_file_write();

unless (inparalog_weight_normalized('test')) {
	inparalog_weight_normalization();
}

construct_graph_all();

write_matrix_index();
%mtx_all=();
execute_MCL();
mcl_backindex();

$setting{'END_TIME'}=`date`;
write_log("\nStart Time: $setting{'START_TIME'}\nEnd Time:   $setting{'END_TIME'}\n");
write_setting();
