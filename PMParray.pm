###
# Package PMParray - deals with a set of similar PMP pipes
#
# Basic pipelining utility

package PMParray;
use strict;
use PMP;

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

    push @{ $self->{PIPES} }, $pipeline;
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


# print the stages - note that this will only print the stages from the 
# first pipe under the assumption that the rest are the same
sub printStages {
    my $self = shift;

    @{ $self->{PIPES} }[0]->printStages();
}

1;
