###
# Package PMP::sge - Poor Man's Pipeline using the SGE batch system
#
# Very similar to the PBS module

package PMP::sge;
use PMP::PMP;
use MNI::MiscUtilities qw(shellquote);

@ISA = ("PMP::PMP");

use strict;

# set the batch queue
sub setQueue {
    my $self = shift;
    my $Q = shift;
    $self->{sgeQueue} = $Q;
}

# set the batch queue options
sub setQueueOptions {
    my $self = shift;
    my $opts = shift;

    $self->{sgeOpts} = $opts;
}

# set the batch hosts
sub setHosts {
    my $self = shift;
    my $hosts = shift;
    $self->{sgeHosts} = $hosts;
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
        $self->{sgePriorityScheme} = $scheme;
    }
}

# overwrite the execStage method and use SGE qsub command to submit jobs
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # set the job name
    my $jobName = "$self->{NAME}-${stageName}";
    $jobName =~ s/;/_/g;
    $jobName =~ s/,/_/g;
    $jobName =~ s/\s/_/g;
    $jobName = "N$jobName" if ($jobName !~ /^[a-zA-Z]/);

    # run the stage in question
    $self->declareStageRunning($stageName);
    my $runningFile = $self->getRunningFile($stageName);

    # now set up the batch job
    my $logFile = $self->getLogFile($stageName);
    my $finishedFile = $self->getFinishedFile($stageName);
    my $failedFile = $self->getFailedFile($stageName);
    my $sgeSub = <<END;

#!/bin/sh
# generated by PMP::sge
#\$ -o $logFile -j y
#\$ -N $jobName
END

    # get the pipe queue
    if (exists $self->{sgeQueue}) {
        $sgeSub .= "#\$ -q $self->{sgeQueue}\n";
    }

#### IS THIS SUPPORTED ON SGE????
#   # get the pipe hosts
#   if (exists $self->{sgeHosts}) {
#       $sgeSub .= "#\$ -l host=$self->{sgeHosts}\n";
#   }

    if (exists $self->{sgePriorityScheme}) {
        # error check for an admittedly unlikely condition
        unless ($self->{STAGES}{$stageName}{'order'} > 1024 ||
                $self->{STAGES}{$stageName}{'order'} < -1023) {
            $sgeSub .= "#\$ -p $self->{STAGES}{$stageName}{'order'}\n";
        }
    }

    $sgeSub .= "cd \$SGE_O_WORKDIR\n";

# now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$sgeSub .= "export ${env}=\"$ENV{$env}\"\n";
    }

    # define the command string
    my $cmdstring = shellquote(@{ $self->{STAGES}{$stageName}{'args'} });

    $sgeSub .= <<END;

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

#open PIPE, ">/tmp/claude/test.sh";
#print PIPE $sgeSub;
#close PIPE;

    my $pipeCmd = "|qsub -S /bin/sh";
    if (exists $self->{sgeOpts}) {
      $pipeCmd .= " $self->{sgeOpts}";
    }
    if ($self->{STAGES}{$stageName}{'sge_opts'}) {
      $pipeCmd .= " $self->{STAGES}{$stageName}{'sge_opts'}";
    }
    if( open PIPE, $pipeCmd) {
      print PIPE $sgeSub;
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


# use SGE qsub command to submit all jobs at once

sub execAllStages {
    my $self = shift;

    # set the job name
    my $jobName = "$self->{NAME}";
    $jobName =~ s/;/_/g;
    $jobName =~ s/,/_/g;
    $jobName =~ s/\s/_/g;
    $jobName = "N$jobName" if ($jobName !~ /^[a-zA-Z]/);
    my $jobLogFile = $self->getLogFile("");

    my $sgeSub = <<END;

#!/bin/sh
# generated by PMP::sge
#\$ -o $jobLogFile -j y
#\$ -N $jobName
END

    # get the pipe queue
    if (exists $self->{sgeQueue}) {
        $sgeSub .= "#\$ -q $self->{sgeQueue}\n";
    }

    $sgeSub .= "cd \$SGE_O_WORKDIR\n";

    # now add the environment to the submission command
    foreach my $env ( keys %ENV ) {
	$sgeSub .= "export ${env}=\"$ENV{$env}\"\n";
    }

    $sgeSub .= <<END;
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

        # define the command string
        my $cmdstring = shellquote(@{ $self->{STAGES}{$stage}{'args'} });

        # now set up the shell script for the batch job
        $sgeSub .= <<END;
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

#open PIPE, ">/tmp/claude/test.sh";
#print PIPE $sgeSub;
#close PIPE;

    my $pipeCmd = "|qsub -S /bin/sh";
    if (exists $self->{sgeOpts}) {
      $pipeCmd .= " $self->{sgeOpts}";
    }
    if( open PIPE, $pipeCmd) {
      print PIPE $sgeSub;
      if (! close PIPE ) {
        warn "ERROR: could not close qsub pipe $self->{NAME}: $!\n";
        warn "Continuing for now, but this pipe might have gone bad.\n";
      }
    } else {
      warn "ERROR: could not open pipe to qsub: $!\n";
    }
}


1;
