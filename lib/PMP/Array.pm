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

# runs all the pipes to completion/crash
sub run {
    my $self = shift;
    my $lockTime = time();
    
    my $allFinished = 0;
    while (! $allFinished) {
	$allFinished = 1;
 	foreach my $pipeline ( @{ $self->{PIPES} } ) {
	    my $status = $pipeline->run();
	    $allFinished = 0 if $status;
	}
	sleep $self->{SLEEP};
    }
    print "\nStopped Processing all pipeline.\n\n";
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

1;
