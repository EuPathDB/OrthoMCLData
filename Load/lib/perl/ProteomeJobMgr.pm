package GUS::Workflow::WorkflowStepInvoker;

use strict;
use GUS::Workflow::SshCluster;
use Carp;

sub getConfig {}

sub getCluster {
    my ($self) = @_;

    if (!$self->{cluster}) {
	my $clusterServer = getConfig('clusterServer');
	my $clusterUser = getConfig('clusterUserName');
	$self->{cluster} = GUS::Workflow::SshCluster->new($clusterServer,
							  $clusterUser,
							  $self);
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

sub error {
    my ($self, $msg) = @_;

    confess("$msg\n\n");
}

sub runClusterTask {
    my ($self, $user, $server, $processIdFile, $logFile, $controllerPropFile, $numNodes, $time, $queue, $ppn) = @_;

    # if not already started, start it up (otherwise the local process was restarted)
    if (!$self->clusterTaskRunning($processIdFile, $user, $server)) {
	my $cmd = "workflowclustertask $propFile $logFile $processIdFile $numNodes $time $queue $ppn";
	$self->runCmd($test, "ssh -2 $user\@$server '/bin/bash -login -c \"$cmd\"'&");
    }


    my $done = $self->runCmd($test, "ssh -2 $user\@$server '/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"'");
    return $done && $done =~ /Done/;
}

sub clusterTaskRunning {
    my ($self, $processIdFile, $user, $server) = @_;

    my $processId = `ssh -2 $user\@$server 'if [ -a $processIdFile ];then cat $processIdFile; fi'`;

    chomp $processId;

    my $status = 0;
    if ($processId) {
      system("ssh -2 $user\@$server 'ps -p $processId > /dev/null'");
      $status = $? >> 8;
      $status = !$status;
    }
    return $status;
}

