###
# Package PMPpbs - Poor Man's Pipeline using the PBS batch system
#
# Almost everything taken from PBS except for the execStage method.

package PMPpbs;
use PMP;
use MNI::MiscUtilities qw(shellquote);

@ISA = ("PMP");

use strict;

# set the batch queue
sub setQueue {
    my $self = shift;
    my $Q = shift;

    if (! ( $Q =~ /short/ || $Q =~ /medium/ || $Q =~ /long/ ) ) {
	die "ERROR: illegal Q $Q - has to be either short, medium, or long\n";
    }
    $self->{pbsQueue} = $Q;
}

# set the batch hosts
sub setHosts {
    my $self = shift;
    my $hosts = shift;

    # some error checking should go here ...

    $self->{pbsHosts} = $hosts;
}

# overwrite the execStage method and use the PBS batch queueing system
# to submit jobs
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # get the pipe queue - use long as default
    my $Q = "long";
    if (exists $self->{pbsQueue}) {
	$Q = $self->{pbsQueue};
    }

    # get the pipe hosts
    my $hosts = "bullcalf:yorick";
    if (exists $self->{pbsHosts}) {
	$hosts = $self->{pbsHosts};
    }

    # set the job name
    my $jobName = "$self->{NAME}:$stageName";
    $jobName =~ s/;/_/g;
    $jobName =~ s/,/_/g;
    $jobName =~ s/\s/_/g;
    $jobName = "N$jobName" if ($jobName !~ /^[a-zA-Z]/);
    $jobName = substr($jobName, 0, 15);

    # run the stage in question
    $self->declareStageRunning($stageName);
    my $runningFile = $self->getRunningFile($stageName);

    # print the stage to stdout
    $self->printStage($stageName);

    # now set up the batch job
    my $logFile = $self->getLogFile($stageName);
    my $finishedFile = $self->getFinishedFile($stageName);
    my $failedFile = $self->getFailedFile($stageName);
    my $pbsSub = <<END;

#!/bin/sh
#PBS -q $Q
#PBS -N $jobName
# send mail on crash
#PBS -m a
# join STDERR and STDOUT
#PBS -j oe
#PBS -o $logFile
#PBS -l host=$hosts

END

$pbsSub .= "cd \$PBS_O_WORKDIR\n";

my $cmdstring = shellquote(@{ $self->{STAGES}{$stageName}{'args'} });

# now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$pbsSub .= "export ${env}=\"$ENV{$env}\"\n";
    }

    # and the actual commands
    $pbsSub .= <<END;
echo "Working directory: " `pwd`
echo \'$cmdstring\'
$cmdstring
if [ "\$?" == "0" ] 
then 
  touch $finishedFile
else 
  touch $failedFile
fi

rm $runningFile

END

open PIPE, "|qsub" or die "ERROR: could not open pipe to qsub: $!\n";
    print PIPE $pbsSub;
    if (! close PIPE ) {
	warn "ERROR: could not close qsub pipe $self->{NAME}: $!\n";
	warn "Continuing for now, but this pipe might have gone bad.\n";
    }
	
}


1;
