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

# runs all the pipes to completion/crash
sub run {
    my $self = shift;
    
    my $allFinished = 0;
    while (! $allFinished) {
	$allFinished = 1;
	foreach my $pipeline ( @{ $self->{PIPES} } ) {
	    my $status ->$pipeline->run();
	    $allFinished = 0 if $status;
	}
	sleep 15;
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

1;
