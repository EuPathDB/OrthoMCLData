package OrthoMCLWebsite::Model::Ppe::ColumnManager;

use strict;
use Data::Dumper;


sub new {
    my ($class, $dbh) = @_;

    my $self = {};

    bless($self, $class);
    $self->{dbh} = $dbh;
    return $self;
}

# we have an ordered list of three letter abbreviations.
# use it to generate a column name for a particular taxon
# each taxon gets two columns, one for protein count, one for taxon count.
# to get column number, double the index.  if taxon count desired, add 1.
sub getColumnName {
    my ($self, $taxonAbbrev, $proteinOrTaxonFlag) = @_;

    my $colNum = $self->getValidTaxonAbbrevs()->{$taxonAbbrev} * 2
	+ ($proteinOrTaxonFlag eq 'T'? 1 : 0);

    return "column$colNum";
}

sub checkTaxonAbbrev {
    my ($self, $taxonAbbrev) = @_;

    return getValidTaxonAbbrevs()->{$taxonAbbrev};
}

# return hash of valid taxon abbrevs.  key -> abbrev, value -> order
sub getValidTaxonAbbrevs {
    my ($self) = @_;

    if (!$self->{validTaxonAbbrevs}) {
      $self->{validTaxonAbbrevs} = {};
      my $sql = "
select three_letter_abbrev
from apidb.orthomcltaxon
order by three_letter_abbrev
";
      my $stmt = $self->{dbh}->prepare($sql) || die "Can't open statement";
      my $orderedTaxonAbbrevs;
      my $validTaxonAbbrevs;
      my $order = 1;
      $stmt->execute();
      while (my $row = $stmt->fetchrow_hashref()) {

	$self->{validTaxonAbbrevs}->{$row->{THREE_LETTER_ABBREV}} = $order++;
      }
    }

    return $self->{validTaxonAbbrevs};
}

1;
