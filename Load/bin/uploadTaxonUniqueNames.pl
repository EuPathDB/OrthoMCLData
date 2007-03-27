#!/usr/bin/perl -w
#
use strict;
use DBI;
use Spreadsheet::ParseExcel;

my $dsn = "DBI:Oracle:orthomcl";
my $dbname = "praveenc";
my $dbpass = "PravWiki";

my $dbh = DBI->connect ($dsn, $dbname, $dbpass)
    or die "Unable to connect to db:$!\n";

my $inputFile = $ARGV[0] ? $ARGV[0]
                : "/home/praveenc/projects/orthomcl/resources_pipeline/datasources_v2_input.xls";
(-r $inputFile)
	or die "Cannot open input file for reading: $!\n";
	 
my $sheet = Spreadsheet::ParseExcel::Workbook->Parse($inputFile)->{Worksheet}->[0];
print $sheet;
foreach my $rowIndex (1 .. $sheet->{MaxRow}) {
    my $row = $sheet->{Cells}[$rowIndex];

    if ($row->[1] && $row->[1]->Value) {
        my $tla = $row->[1]->Value;
        my $ncbiTaxonId = $row->[10]->Value;
        my $sql = "update sres.TaxonName set unique_name_variant=\'$tla\' "
                    . " where taxon_id in (select taxon_id from sres.taxon "
                    . " where ncbi_tax_id=\'$ncbiTaxonId\')";
        #print $sql . "\n";
        my $sth = $dbh->prepare (qq{$sql});
        $sth->execute 
        	or die "Failed to execute update query for $tla\n";
        
        print "Successfully update $tla\n";
    }
}
