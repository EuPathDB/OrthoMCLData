package GUS::Workflow::WorkflowStepInvoker;

use strict;
use GUS::Workflow::SshCluster;
sub getConfig {}

sub getCluster {
    my ($self) = @_;

    if (!$self->{cluster}) {
	my $clusterServer = $getConfig('clusterServer');
	my $clusterUser = $getConfig('clusterUserName');
	$self->{cluster} = GUS::Workflow::SshCluster->new($clusterServer,
							      $clusterUser,
							      $self);
	} else {
	    $self->{cluster} = GUS::Workflow::NfsCluster->new($self);
	}
    }
    return $self->{cluster};
}

sub runCmd {
    my ($self) = @_;

    my $output = `$cmd 2>> $err`;
    my $status = $? >> 8;
    $self->error("Failed with status $status running: \n$cmd") if ($status);
    return $output;
}

