###
# Package PMPpbs - Poor Man's Pipeline using the PBS batch system
#
# Almost everything taken from PBS except for the execStage method.

package PMPpbs;
use PMP;

@ISA = ("PMP");

use strict;

# overwrite the execStage method and use the PBS batch queueing system
# to submit jobs
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # run the stage in question
    $self->declareStageRunning($stageName);

    # now set up the batch job
    my $logFile = $self->getLogFile($stageName);
    my $finishedFile = $self->getFinishedFile($stageName);
    my $failedFile = $self->getFailedFile($stageName);
    my $pbsSub = <<END;

#!/bin/sh
#PBS -q short
#PBS -N $stageName
# send mail on crash
#PBS -m a
# join STDERR and STDOUT
#PBS -o $logFile
#PBS -l host=bullcalf

END

# now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$pbsSub .= "export ${env}=\"$ENV{$env}\"\n";
    }

    # and the actual commands
    $pbsSub .= <<END;

@{ $self->{STAGES}{$stageName}{'args'} }
if [ "\$?" == "0" ] 
then 
  touch $finishedFile
else 
  touch $failedFile
fi

END

open PIPE, "|qsub" or die "ERROR: could not open pipe to qsub: $!\n";
    print PIPE $pbsSub;
    close PIPE or die "ERROR: could not close qsub pipe: $!\n";
}


1;
