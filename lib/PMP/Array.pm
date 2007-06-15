###
# Package PMParray - deals with a set of similar PMP pipes
#
# Basic pipelining utility

package PMP::Array;
use strict;
use PMP::PMP;

# the constructor
sub new {
    # allow for inheritance
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};

    ### the class attributes ###

    # holds the pipelines
    $self->{PIPES} = [];
    # default waiting time
    $self->{SLEEP} = 15;
    # maximum number of jobs in queue (ridiculously large by default)
    $self->{MAXQUEUED} = 10e6;
    # granularity of scheduling of stages
    # 0 - stage by stage
    # 1 - all stages at once
    $self->{GRANULARITY} = 0;

    # bring the class into existence
    bless($self, $class);
    return $self;
}

# adds a pipe to the pipearray
sub addPipe {
    my $self = shift;
    my $pipeline = shift;
    my $unique = 1;

    # Test that pipe does not already exist.
    foreach my $pipe ( @{ $self->{PIPES} } ) {
        if( $pipe->{NAME} eq $pipeline->{NAME} ) {
            $unique = 0;
            last;
        }
    }
    if( $unique ) {
      push @{ $self->{PIPES} }, $pipeline;
      print "Added pipe $pipeline->{NAME}\n";
    } else {
      print "Ignored duplicate pipe $pipeline->{NAME}\n";
    }
    return $unique;
}

# get or set the sleep time
sub sleepTime {
    my $self = shift;
    if (@_) { $self->{SLEEP} = shift; }
    return $self->{SLEEP};
}

# get or set the maximum number of jobs to keep queued
sub maxQueued {
    my $self = shift;
    if (@_) { $self->{MAXQUEUED} = shift; }
    return $self->{MAXQUEUED};
}

# set granularity of scheduling of stages
sub setGranularity {
    my $self = shift;
    $self->{GRANULARITY} = shift;
}

# runs all the pipes to completion/crash
sub run {
    my $self = shift;
    my $nPipes = $#{ $self->{PIPES} } + 1;
    my $i = 0;

    # Assume all jobs are potentially running
    my @status = [];
    while ($i < $nPipes) {
        push @status, 1;
        $i++;
    }

    my $allFinished = 0;
    while (! $allFinished) {
	$allFinished = 1;
	my $nQueued = 0;

	$i = 0;

	# Loop through all pipes, but go to sleep when the maximum
	# number of queued jobs has been reached
	while (($nQueued < $self->{MAXQUEUED}) && ($i < $nPipes)) {
	    my $pipeline = $self->{PIPES}[$i];

            if( @status[$i] ) {
	        @status[$i] = $pipeline->run( $self->{GRANULARITY} );
	        if (@status[$i]) {
		    $allFinished = 0;
                    if( $self->{GRANULARITY} == 0 ) {
                      $nQueued += $pipeline->nQueued();
                    } else {
                      $nQueued += 1;
		    }
                }
            }

	    $i++;
	}
	sleep $self->{SLEEP};
    }
    print "\nStopped processing all pipelines.\n\n";

    print "\nChecking for undeleted lock files...\n";
    cleanup();

}

# updates the status of all pipes based on filenames
sub updateFromFiles {
    my $self = shift;
    
    foreach my $pipe ( @{ $self->{PIPES} } ) {
	$pipe->statusFromFiles();
    }
}

# print the unfinished stages
sub printUnfinished {
    my $self = shift;

    foreach my $pipe ( @{ $self->{PIPES} } ) {
	$pipe->printUnfinished();
    }
}

# reset all stages from a certain point in the dependency tree
sub resetFromStage {
    my $self = shift;
    my $stageName = shift;

    foreach my $pipe ( @{ $self->{PIPES} } ) {
	$pipe->resetFromStage($stageName);
    }
}

# reset all stages
sub resetAll {
    my $self = shift;
    my $stageName = shift;

    foreach my $pipe ( @{ $self->{PIPES} } ) {
	$pipe->resetAll();
    }
}

# register the programs - note that this will only register the
# programs for the first pipe, assuming the rest to be the same.
sub registerPrograms {
    my $self = shift;
    
    @{ $self->{PIPES} }[0]->registerPrograms();
}

# make a dot dependency graph - will only do so for the first pipe
sub createDotGraph {
    my $self = shift;
    my $filename = shift;
    @{ $self->{PIPES} }[0]->createDotGraph($filename);
}

# make a dot dependency graph - will only do so for the first pipe
sub createFilenameDotGraph {
    my $self = shift;
    my $filename = shift;
    my $substring = "";
    if (@_) { $substring = shift; }

    @{ $self->{PIPES} }[0]->createFilenameDotGraph($filename, $substring);
}

# print the status of all pipelines. Optional second argument is a
# filename, in which case the status is printed to file rather than to
# stdout
sub printPipelineStatus {
    my $self = shift;
    my $filename = undef;
    if (@_) { $filename = shift; }

    if ($filename) {
	open REPORT, ">$filename" or die "ERROR opening $filename: $!\n";
    }

    foreach my $pipeline ( @{ $self->{PIPES} } ) {
	my $status = $pipeline->getPipelineStatus();
	if ($filename) { 
	    print REPORT "$status\n";
	}
	else {
	    print "$status\n";
	}
    }

    if ($filename) { close REPORT; }
}

# create a CVS file reporting on status of all stages
sub printStatusReport {
    my $self = shift;
    my $filename = shift;

    open REPORT, ">$filename" or die 
	"ERROR opening report file $filename for writing: $!\n";

    # write the header
    @{ $self->{PIPES} }[0]->printStatusReportHeader(*REPORT);

    # write reports for all pipelines
    foreach my $pipeline ( @{ $self->{PIPES} } ) {
	$pipeline->printStatusReport(*REPORT);
    }

    close REPORT;
}
	
# only runs up to a specified stage in pipeline.
sub subsetToStage {
    my $self = shift;
    my $filename = shift;

    foreach my $pipeline( @{ $self->{PIPES} } ) {
	$pipeline->subsetToStage( $filename );
    }
}

# print the stages - note that this will only print the stages from the 
# first pipe under the assumption that the rest are the same
sub printStages {
    my $self = shift;

    @{ $self->{PIPES} }[0]->printStages();
}


# Remove lock files
sub cleanup {
    my $self = shift;

    foreach my $pipeline ( @{ $self->{PIPES} } ) {
	$pipeline->cleanLockFile();
    }
}

1;


