###
# Package PMPpbs - Poor Man's Pipeline using the PBS batch system
#
# Almost everything taken from PBS except for the execStage method.

package PMP::pbs;
use PMP::PMP;
use MNI::MiscUtilities qw(shellquote);

@ISA = ("PMP::PMP");

use strict;

# set the batch queue
sub setQueue {
    my $self = shift;
    my $Q = shift;
    $self->{pbsQueue} = $Q;
}

# set the batch queue options
sub setQueueOptions {
    my $self = shift;
    my $opts = shift;
    $self->{pbsOpts} = $opts;
}

# set the batch hosts
sub setHosts {
    my $self = shift;
    my $hosts = shift;

    # some error checking should go here ...

    $self->{pbsHosts} = $hosts;
}

# set the priority scheme
sub setPriorityScheme {
    my $self = shift;
    my $scheme = shift;

    # only one allowed so far: later-stages, which give priority to
    # later stages over earlier stages.
    if (! $scheme =~ /later-stages/ ) {
        warn "Warning: illegal priority scheme $scheme. Ignoring request.\n";
    } else {
        $self->{pbsPriorityScheme} = $scheme;
    }
}
    

# overwrite the execStage method and use the PBS batch queueing system
# to submit jobs
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # set the job name
    my $jobName = "$self->{NAME}:${stageName}";
    $jobName =~ s/;/_/g;
    $jobName =~ s/,/_/g;
    $jobName =~ s/\s/_/g;
    $jobName = "N$jobName" if ($jobName !~ /^[a-zA-Z]/);
    $jobName = substr($jobName, 0, 15);

    # run the stage in question
    $self->declareStageRunning($stageName);
    my $runningFile = $self->getRunningFile($stageName);

    # now set up the batch job
    my $logFile = $self->getLogFile($stageName);
    my $finishedFile = $self->getFinishedFile($stageName);
    my $failedFile = $self->getFailedFile($stageName);
    my $pbsSub = <<END;

#!/bin/sh
#PBS -N $jobName
# send mail on crash
#PBS -m a
# join STDERR and STDOUT
#PBS -j oe
#PBS -o $logFile
END

    # get the pipe queue
    if (exists $self->{pbsQueue}) {
	$pbsSub .= "#PBS -q $self->{pbsQueue}\n";
    }

    # get the pipe hosts
    if (exists $self->{pbsHosts}) {
	$pbsSub .= "#PBS -l host=$self->{pbsHosts}\n";
    }

    if (exists $self->{pbsPriorityScheme}) {
	# error check for an admittedly unlikely condition
	unless ($self->{STAGES}{$stageName}{'order'} > 1024 ||
		$self->{STAGES}{$stageName}{'order'} < -1023) {
	    $pbsSub .= "#PBS -p $self->{STAGES}{$stageName}{'order'}\n";
	}
    }

    $pbsSub .= "cd \$PBS_O_WORKDIR\n";

# now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$pbsSub .= "export ${env}=\"$ENV{$env}\"\n";
    }

    # define the command string, shellquoting it if so desired
    my $cmdstring = shellquote(@{ $self->{STAGES}{$stageName}{'args'} });

    # and the actual commands
    $pbsSub .= <<END;

echo "Start running on: " `uname -s -n -r` " at " `date`
echo "$cmdstring"
$cmdstring
if [ "\$?" == "0" ] 
then 
  touch $finishedFile
else 
  touch $failedFile
fi

rm -f $runningFile

END

    if( open PIPE, "|qsub $self->{pbsOpts}" ) {
      print PIPE $pbsSub;
      if (! close PIPE ) {
	warn "ERROR: could not close qsub pipe $self->{NAME}: $!\n";
	warn "Continuing for now, but this pipe might have gone bad.\n";
      }
    } else {
      `touch $failedFile`;
      unlink $runningFile;
      warn "ERROR: could not open pipe to qsub: $!\n";
    }
	
}

# use PBS batch queueing system to submit all jobs at once

sub execAllStages {
    my $self = shift;

    # set the job name
    my $jobName = "$self->{NAME}";
    $jobName =~ s/;/_/g;
    $jobName =~ s/,/_/g;
    $jobName =~ s/\s/_/g;
    $jobName = "N$jobName" if ($jobName !~ /^[a-zA-Z]/);
    $jobName = substr($jobName, 0, 15);
    my $jobLogFile = $self->getLogFile("");

    my $pbsSub = <<END;
#!/bin/sh
#PBS -N $jobName
# send mail on crash
#PBS -m a
# join STDERR and STDOUT
#PBS -j oe
#PBS -o $jobLogFile
END

    # get the pipe queue
    if (exists $self->{pbsQueue}) {
	$pbsSub .= "#PBS -q $self->{pbsQueue}\n";
    }

    # get the pipe hosts
    if (exists $self->{pbsHosts}) {
	$pbsSub .= "#PBS -l host=$self->{pbsHosts}\n";
    }

    $pbsSub .= "cd \$PBS_O_WORKDIR\n";

    # now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$pbsSub .= "export ${env}=\"$ENV{$env}\"\n";
    }
    $pbsSub .= <<END;
END

    # write out the stages

    $self->sortStages() unless $self->{isSorted};

    foreach my $stage ( @{ $self->{sortedStages} } ) {
      if (! $self->{STAGES}{$stage}{'finished'} ) {

        # run the stage in question
        $self->declareStageRunning($stage);
        my $runningFile = $self->getRunningFile($stage);

        my $logFile = $self->getLogFile($stage);
        my $finishedFile = $self->getFinishedFile($stage);
        my $failedFile = $self->getFailedFile($stage);

        # define the command string, shellquoting it if so desired
        my $cmdstring = shellquote(@{ $self->{STAGES}{$stage}{'args'} });

        # now set up the shell script for the batch job
        $pbsSub .= <<END;
echo "Start running on: " `uname -s -n -r` " at " `date` \>\& $logFile
echo "$cmdstring" \>\> $logFile 2\>\&1
$cmdstring \>\> $logFile 2\>\&1
if [ "\$?" == "0" ] 
then 
  touch $finishedFile
else 
  touch $failedFile
  rm -f $runningFile
  exit 1
fi
rm -f $runningFile
END
      }
    }

#open PIPE, ">/tmp/test.sh";
#print PIPE $pbsSub;
#close PIPE;

    if( open PIPE, "|qsub $self->{pbsOpts}" ) {
      print PIPE $pbsSub;
      if (! close PIPE ) {
	warn "ERROR: could not close qsub pipe $self->{NAME}: $!\n";
	warn "Continuing for now, but this pipe might have gone bad.\n";
      }
    } else {
      warn "ERROR: could not open pipe to qsub: $!\n";
    }
}


1;
