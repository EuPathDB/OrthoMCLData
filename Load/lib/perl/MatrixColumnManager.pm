package OrthoMCLData::Load::MatrixColumnManager;

use strict;
use Data::Dumper;

# an object that maps species and clades onto the generic columns
# in the apidb.grouptaxonmatrix table

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
sub getColumnNumber {
    my ($self, $taxonAbbrev, $proteinOrTaxonFlag) = @_;

    my $colNum = $self->getValidTaxonAbbrevs()->{$taxonAbbrev} * 2 + 1
	+ ($proteinOrTaxonFlag eq 'T'? 1 : 0);

    return $colNum;
}

sub getColumnName {
    my ($self, $taxonAbbrev, $proteinOrTaxonFlag) = @_;

    my $colNum = $self->getColumnNumber($taxonAbbrev, $proteinOrTaxonFlag);
    return "column$colNum";
}

sub getTaxonAbbrev {
  my ($self, $columnName) = @_;

  $columnName =~ /column(\d+)/ or die "'$columnName' is an invalid column name";
  my $colNum = $1 - 1;

  my $index = int($colNum / 2);
  my $taxonAbbrev = $self->getValidTaxonAbbrevs()->[$index];


  return ($taxonAbbrev, $colNum % 2 == 0? 'P' : 'T');
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
      my $order = 0;
      $stmt->execute();
      while (my $row = $stmt->fetchrow_hashref()) {

	$self->{validTaxonAbbrevs}->{$row->{THREE_LETTER_ABBREV}} = $order++;
      }
#      $self->printFullMapping();   # for debugging
    }

    return $self->{validTaxonAbbrevs};
}

sub printFullMapping {
  my ($self) = @_;
  foreach my $tax (keys %{$self->{validTaxonAbbrevs}}) {
    print "$tax P: " . $self->getColumnName($tax, 'P') . "\n";
    print "$tax T: " . $self->getColumnName($tax, 'T') . "\n";
  }
}
1;
