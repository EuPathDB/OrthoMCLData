package OrthoMCLData::Load::Plugin::ExtractExternalAASequences;

@ISA = qw (GUS::PluginMgr::Plugin);

use GUS::PluginMgr::Plugin;
use GUS::Model::DoTS::ExternalAASequence;

use lib "$ENV{$GUS_HOME}/lib/perl";
use strict;

$| = 1;

sub getArgsDeclaration {
	my $argsDeclaration = 
		[
		booleanArg ({
			name=>'noUpdate',
			descr=>'Do notFlags the sequences as good/short/MSC',
			isList=>0,
			reqd=>0,
		}),

		stringArg ({
			name=>'minSeqLength',
			descr=>'AA Sequences less than this size are flagged as short (default 10)',
			isList=>0,
			reqd=>0,
		}),

		stringArg ({
			name=>'sequenceFile',
			descr=>'Output filename for AA sequences',
			constraintFunc=>undef,
			isList=>0,
			reqd=>1,
		}),

		];

	return $argsDeclaration;
}

sub getDocumentation {
	my $purposeBrief = <<PURPOSEBRIEF;
Flag the sequences with 'good', 'MSC' (for Multiple Stop Codons), or 'short', and select only the 'good' sequences and append them into one file, preparing for BLAST.
PURPOSEBRIEF

	my $purpose = <<PLUGIN_PURPOSE;
Flag the sequences with 'good', 'MSC' (for Multiple Stop Codons), or 'short', and select only the 'good' sequences and append them into one file, preparing for BLAST. Some of the sequences may contain multiple stop codons, but may not be flagged as 'MSC' because their effect may be negligible, and we would want to use those.
PLUGIN_PURPOSE
	
	my $tablesAffected = [
		['DoTS.ExternalAASequence'=>
			'Uses the NAME field to flag each sequence as \'good\', \'MSC\', or \'short\'']
	];

	my $tablesDependedOn = [
		['DoTS.ExternalAASequence'=>
			'Fetch AA Sequences, filtered by the NAME field.'
		]
	];

	my $howToRestart = <<PLUGIN_RESTART;
Restarted plugin will recompute the flags, and overwrites the output file.
PLUGIN_RESTART

	my $failureCases = <<PLUGIN_FAILURE_CASES;
None.
PLUGIN_FAILURE_CASES
	
	my $notes = <<PLUGIN_NOTES;
No additional notes.
PLUGIN_NOTES
  my $documentation = { purposeBrief => $purposeBrief,
              purpose => $purpose,
              tablesAffected => $tablesAffected,
              tablesDependedOn => $tablesDependedOn,
              howToRestart => $howToRestart,
              failureCases => $failureCases,
              notes => $notes,
			};
																						    return ($documentation);
}

sub new {
   my $class = shift;
   my $self = {};
   bless($self, $class);
  
      my $documentation = &getDocumentation();
      my $args = &getArgsDeclaration();

      $self->initialize({requiredDbVersion => 3.6,
                 cvsRevision => '$Revision: 5129 $',
                 cvsTag => '$Name:  $',
                 name => ref($self),
                 revisionNotes => '',
                 argsDeclaration => $args,
                 documentation => $documentation
                });
    
   return $self;
} 

sub run {
	my ($self) = @_;

	$self->logAlgInvocationId;
	$self->logCommit;

	my $minSeqLength = $self->getArg ("minSeqLength") 
						? $self->getArg ("minSeqLength")
						: 10;
	
	my $dbh = $self->getQueryHandle();

	my $flagSql = <<FLAGSQL;
	SELECT as.aa_sequece_id, as.sequence
	FROM DoTs.ExternalAASequence as
FLAGSQL

	my $fetchSql = <<FETCHSQL;
	SELECT as.aa_sequence_id, as.sequence, as.taxon_id
	FROM DoTs.ExternalAASequence as
	WHERE as.molecule_type = 'good'
FETCHSQL

	my $fetchSth = $dbh->prepare ($fetchSql);
	
	while (my ($aaId, $seq, $taxId) = $fetchSth->fetchrow()) {
				
		
	}


	$self->setResultDescr($resultDescrip);
	$self->logData ($resultDescrip);

}

sub isShort {
	my ($self, $aaSeq, $minSeqLength) = @_;
	return (length ($aaSeq) < $minSeqLength);
}

sub isMSC {

}


1;

