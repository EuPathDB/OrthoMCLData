package OrthoMCLData::Load::Plugin::InsertGroupDomains;

@ISA = qw(GUS::PluginMgr::Plugin);

# ----------------------------------------------------------------------

use strict;
use GUS::PluginMgr::Plugin;
use FileHandle;

use GUS::Model::ApiDB::OrthomclGroupDomain;

use ApiCommonData::Load::Util;
use Data::Dumper;

require Exporter;

my $argsDeclaration =
[
];


my $purpose = <<PURPOSE;
Calculate the relevant pfam domain keywords for each ortholog group in the DB, 
and insert the domains (with frequencies) into the OrthomclGroupDomain table.
PURPOSE

my $purposeBrief = <<PURPOSE_BRIEF;
Insert the group domain keywords into the OrthomclGroupDomain table  
PURPOSE_BRIEF

my $notes = <<NOTES;
NOTES

my $tablesAffected = <<TABLES_AFFECTED;
ApiDB.OrthomclGroupDomain,
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

# ---------------------------------------------------------------------

sub new {
  my ($class) = @_;
  my $self = {};
  bless($self,$class);

  $self->initialize({ requiredDbVersion => 3.5,
                      cvsRevision       => '$Revision: 1 $',
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
    my $sql_groups = "SELECT og.ortholog_group_id, og.number_of_members
                      FROM apidb.OrthologGroup og";
    my $sql_domains_per_group = "SELECT ogs.aa_sequence_id, 
                                        dbaf.db_ref_id, dbref.remark 
                                 FROM apidb.OrthologGroupAaSequence ogs, 
                                      dots.DomainFeature df,
                                      dots.DbRefAaFeature dbaf,
                                      sres.DbRef
                                 WHERE ogs.aa_sequence_id = df.aa_sequence_id
                                   AND df.aa_feature_id = dbaf.aa_feature_id
                                   AND dbaf.db_ref_id = dbref.db_ref_id
                                   AND ogs.ortholog_group_id = ?";
    my $ps_groups = $dbh->prepare($sql_groups);
    my $ps_domains_per_group = $dbh->prepare($sql_domains_per_group);
    
    my $count = 0;
    
    $ps_groups->execute();
    while (my ($group_id, $num_seqs) = $ps_groups->fetchrow_array()) {
        my %sequence_domain;
        my %domain_texts;

        $ps_domains_per_group->execute($group_id);
        while (my ($seq_id, $dbref_id, $remark) = $ps_domains_per_group->fetchrow_array()) {
	        $sequence_domain{$seq_id}->{$dbref_id}=1;
	        if ($remark) {
                $domain_texts{$dbref_id} = $remark;
            }
        }
        my %domains = %{DomainFreq($num_seqs, \%sequence_domain)};
        foreach my $d (keys %domains) {
            if ($domain_texts{$d}) {
                my $domain = GUS::Model::ApiDB::OrthomclGroupDomain->new();

                $domain->setOrthologGroupId($group_id);
                $domain->setDescription($domain_texts{$d});
                $domain->setFrequency($domains{$d});

                $domain->submit();
	            $self->undefPointerCache();  
            }
        }
        $count++;
        if ($count % 1000 == 0) {
            print STDERR "$count groups processed.\n";
        }
    }

    return "Done adding group domain keywords.";
}

sub undoTables {
    my ($self) = @_;
    
    return ('ApiDB.OrthomclGroupDomain',
	    );
}

# ----------------------------------------------------------------------

sub DomainFreq {
# SELECT sequence2domain.sequence_id, domain_id FROM sequence2domain
#INNER JOIN sequence USING (sequence_id) WHERE sequence.orthogroup_id = ?

# SELECT description FROM domain WHERE domain_id = ?;

	my $group_size=$_[0];
	my %sequence_domain = %{$_[1]};

	my $return_dom_num=3;
	my $freq_cutoff=0.5;

	my %dom_freq;
	foreach my $s (keys %sequence_domain) {
		foreach my $d (keys %{$sequence_domain{$s}}) {
			$dom_freq{$d}++;
		}
	}

	my %return_freq;
	my $return_dom_count=0;

	foreach my $d (sort {$dom_freq{$b}<=>$dom_freq{$a}} keys %dom_freq) {
		my $f = $dom_freq{$d}/$group_size;
		next unless ($f>$freq_cutoff);
		$return_dom_count++;
		last if ($return_dom_count>$return_dom_num);
		$return_freq{$d}=$f;
	}
	return \%return_freq;
}

1;
