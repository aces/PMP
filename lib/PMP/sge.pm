###
# Package PMP::sge - Poor Man's Pipeline using the SGE batch system
#
# Very similar to the PBS module

package PMP::sge;
use PMP::PMP;
use File::Temp qw/ tempdir /;
use MNI::MiscUtilities qw(shellquote);

@ISA = ("PMP::PMP");

use strict;

# set the submission command
sub setCommand {
    my $self = shift;
    my $Q = shift;
    $self->{sgeCommand} = $Q;
}

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
    # don't include vars with () in them and remove '\n' inside names (CL).
    foreach my $env ( keys %ENV ) {
        if( !( ${env} =~ m/\(\)/ ) ) {
            my $val = $ENV{$env};
            $val =~ s/\n//g;
            $sgeSub .= "export ${env}=\"${val}\"\n";
        }
    }

    # define the command string
    my $cmdstring = shellquote(@{ $self->{STAGES}{$stageName}{'args'} });

    $sgeSub .= <<END;

echo "Start running on: " `uname -s -n -r` " at " `date`
echo "$cmdstring"
$cmdstring
if test "\$?" -eq "0"
#if [ "\$?" == "0" ] ## broken on Ubuntu Hardy and up.
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
    if (! (exists $self->{sgeCommand}) ) {
      $self->{sgeCommand} = "qsub";
    }

    my $pipeCmd = "|$self->{sgeCommand} -S /bin/sh";
    if (exists $self->{sgeOpts}) {
      $pipeCmd .= " $self->{sgeOpts}";
    }
    if ($self->{STAGES}{$stageName}{'sge_opts'}) {
      $pipeCmd .= " $self->{STAGES}{$stageName}{'sge_opts'}";
    }
    if( open PIPE, $pipeCmd) {
      print PIPE $sgeSub;
      if (! close PIPE ) {
        warn "ERROR: could not close $self->{sgeCommand} pipe $self->{NAME}: $!\n";
        warn "Continuing for now, but this pipe might have gone bad.\n";
      }
    } else {
      `touch $failedFile`;
      unlink $runningFile;
      warn "ERROR: could not open pipe to $self->{sgeCommand}: $!\n";
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
    # don't include vars with () in them and remove '\n' inside names (CL).
    foreach my $env ( keys %ENV ) {
        if( !( ${env} =~ m/\(\)/ ) ) {
            my $val = $ENV{$env};
            $val =~ s/\n//g;
            $sgeSub .= "export ${env}=\"${val}\"\n";
        }
    }

    $sgeSub .= <<END;
END

    # write out the stages

    $self->sortStages() unless $self->{isSorted};

    foreach my $stage ( @{ $self->{sortedStages} } ) {

      # check to make sure that this stage is in the subset of stages to be run

      if ( ( ! $self->{STAGES}{$stage}{'finished'} ) &&
           ( $self->{runAllStages} == 1 || $self->{stagesSubset}{$stage} ) ) {

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

    # Submit the job this way to avoid the 100k command line
    # limit in sh if commands are piped to qsub. Using a file
    # removes the size restriction on the job script.

    my $tmpdir = &tempdir( "pmp-XXXXXXXX", TMPDIR => 1, CLEANUP => 1 );
    my $job_script = "${tmpdir}/${jobName}.sh";
    open PIPE, ">${job_script}";
    print PIPE $sgeSub;
    close PIPE;

    if (! (exists $self->{sgeCommand}) ) {
      $self->{sgeCommand} = "qsub";
    }
    my @args = ( "$self->{sgeCommand}", "-S", "/bin/sh" );
    if( exists $self->{sgeOpts} ) {
      push @args, split( /\s+/, $self->{sgeOpts} );
    }
    push @args, ( $job_script );
    if( system( @args ) ) {
      warn "ERROR: could not $self->{sgeCommand} pipe $self->{NAME}: $!\n";
      warn "Continuing for now, but this pipe might have gone bad.\n";
      unlink( $job_script );
    }
}


1;
