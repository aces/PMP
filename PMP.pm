###
# Package PMP - Poor Man's Pipeline
#
# Some basic pipelining that is elegant to use and not dependent on
# too many modules or databases.

package PMP;
use strict;
use MNI::Spawn;
use MNI::Startup;


# the constructor
sub new {
    # allow for inheritance
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};

    ######################
    # the class attributes
    ######################

    # holds the stages
    $self->{STAGES} = {};
    # the method to be used to execute stages
    $self->{METHOD} = "spawn";
    # the name of this particular pipeline
    $self->{NAME} = "noname";
    # directory to hold serialization objects
    $self->{STATUSDIR} = "/tmp";
    # holds a hash of the batch options
    $self->{BATCH} = {};
    # holds the number of the current stage being added
    $self->{CURRENT} = 0;
    # holds a sorted list of the stages
    $self->{sortedStages} = [];
    # whether the sorted list of stages is up to date
    $self->{isSorted} = 0;
    # list of stages to be executed in the next iteration
    $self->{toBeExecuted} = [];

    # bring the class into existence
    bless($self, $class);
    return $self;
}

# get or set the pipeline name
sub name {
    my $self = shift;
    if (@_) { $self->{NAME} = shift; }
    return $self->{NAME};
}

# get or set the directory to be used for the status file
sub statusDir {
    my $self = shift;
    if (@_) { $self->{STATUSDIR} = shift; }
    return $self->{STATUSDIR};
}

# add a stage to the pipeline
sub addStage {
    my $self = shift;
    # should be a hash reference
    my $stage = shift;

    # check to make sure that all the prereq elements have been specified
    if (! exists $$stage{'name'} ) {
	die "ERROR specifying stage: no name specified!\n";
    }
    if (! exists $$stage{'inputs'} ) {
	die "ERROR defining stage $$stage{'name'}: no inputs specified!\n";
    }
    if (! exists $$stage{'outputs'} ) {
	die "ERROR defining stage $$stage{'name'}: no outputs specified!\n";
    }
    if (! exists $$stage{'args'} ) {
	die "ERROR defining stage $$stage{'name'}: no args specified!\n";
    }

    # TODO: check whether the various items are of the correct datatype

    # warn if a stage with the same name is already in the pipeline
    if (exists $self->{STAGES}{$$stage{'name'}}) {
	warn "WARNING stage $$stage{'name'} already exists in the pipeline!\n";
    }

    # the stage definition has passed the various tests - now add some 
    # internal record keeping elements to it.

    $$stage{'order'} = $self->{CURRENT};
    $self->{CURRENT}++;
    $$stage{'started'} = 0;
    $$stage{'finished'} = 0;
    $$stage{'failed'} = 0;
    $$stage{'runnable'} = 0;

    # add it to the stage list of the pipeline
    $self->{STAGES}{$$stage{'name'}} = $stage;

    # new stage was added - assume that they are now out of order
    $self->{isSorted} = 0;
}

# created a sorted list of the various stages
sub sortStages {
    my $self = shift;

    # reinitialize the array to empty
    delete $self->{sortedStages};

    my @keys = keys %{ $self->{STAGES} };
    my @insertedStages;
    my @uninsertedStages;
    my $currentInsertion = 0;
    
    # stage 1: find all the stages with no prereqs at all
    foreach my $key ( @keys ) {
	if (! exists $self->{STAGES}{$key}{'prereqs'} ) {
	    push @insertedStages, $key;
	    $currentInsertion++;
	}
	else {
	    push @uninsertedStages, $key;
	}
    }

    # stage 2: insert stages which only depend on already inserted stages
    while ($#insertedStages < $#keys) {
	my @tmp = []; # temporarily hold uniserteable stages
	foreach my $key ( @uninsertedStages ) {
	    my $validInsertion = 1;
	    foreach my $stage ( @{ $self->{STAGES}{$key}{'prereqs'} } ) {
		if (! grep( $stage, @insertedStages ) ) {
		    $validInsertion = 0;
		    last;
		}
	    }
	    if ($validInsertion == 1) { 
		push @insertedStages, $key;
	    }
	    else {
		push @tmp, $key;
	    }
	}

	@uninsertedStages = @tmp;
    }

    # copy the ordered list back to the class namespace
    $self->{sortedStages} = \@insertedStages;

    # reset the order key for each of the individual stages
    my $i = 0;
    foreach my $key ( @{ $self->{sortedStages} } ) {
	$self->{STAGES}{$key}{'order'} = $i;
	$i++;
    }

    # indicate that the list is now sorted
    $self->{isSorted} = 1;
}
    
# update status of all stages
sub updateStatus {
    my $self = shift;
    delete $self->{toBeExecuted};
    my @toBeExecuted;

    foreach my $key ( keys %{ $self->{STAGES} } ) {
	if ($self->updateStageStatus($key)) {
	    push @toBeExecuted, $key;
	}
    }
    $self->{toBeExecuted} = \@toBeExecuted;

    # returns 0 if there are no more stages to be executed
    return $#toBeExecuted + 1;
}

# run the next iteration
sub run {
    my $self = shift;
    
    foreach my $key ( @{ $self->{toBeExecuted} } ) {
	$self->execStage($key);
    }
    return $self->updateStatus();
}

# update the status of a stage
sub updateStageStatus {
    my $self = shift;
    my $stageName = shift;

    my $runnable = 1;

    # check to make sure that it has neither finished nor failed
    if ($self->{STAGES}{$stageName}{'finished'} ||
	$self->{STAGES}{$stageName}{'failed'} ) {
	$runnable = 0;
    }
    # if a stage has no prereqs it is runnable
    elsif (! exists $self->{STAGES}{$stageName}{'prereqs'} ) {
	$self->{STAGES}{$stageName}{'runnable'} = 1;
    }
    # same if all the prereqs are finished 
    else { 
	foreach my $stage ( @{ $self->{STAGES}{$stageName}{'prereqs'} } ) {
	    if ($self->{STAGES}{$stage}{'finished'} == 0) {
		$runnable = 0;
		last;
	    }
	}
	$self->{STAGES}{$stageName}{'runnable'} = $runnable;
    }
    return $runnable;
}

# exec a stage through the Spawn interface
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # the status filenames
    my $baseStatus = "$self->{STATUSDIR}/$self->{NAME}.${stageName}";
    my $runningFile = "${baseStatus}.running";
    my $finishedFile = "${baseStatus}.finished";
    my $failedFile = "${baseStatus}.failed";
    my $logFile = "${baseStatus}.log";

    # run the stage in question
    Spawn(["touch", $runningFile]);
    my $status = Spawn($self->{STAGES}{$stageName}{'args'}, 
		       stdout => $logFile);
    if ($status != 0) {
	Spawn(["touch", $failedFile]);
	$self->{STAGES}{$stageName}{'failed'} = 1;
    }
    else {
	Spawn(["touch", $finishedFile]);
	$self->{STAGES}{$stageName}{'finished'} = 1;
    }
    unlink $runningFile;
}

# print the definition of a single stage
sub printStage {
    my $self = shift;
    my $stageName = shift;

    if (exists $self->{STAGES}{$stageName}) {
	my $stage = $self->{STAGES}{$stageName};
	
	print "======== $$stage{'order'}: $stageName ========\n";
	print "Inputs: @{ $$stage{'inputs'} }\n";
	print "Outputs: @{ $$stage{'outputs'} }\n";
	print "Args: @{ $$stage{'args'} }\n";
	print "Prereqs: @{ $$stage{'prereqs'} }\n" 
	    if exists $$stage{'prereqs'};
	print "Has started\n" if $$stage{'started'};
	print "Has finished\n" if $$stage{'finished'};
	print "Is ready to run\n" if $$stage{'runnable'};
	print "\n";
    }
    else {
	warn "WARNING: cannot print stage $stageName because it does not exist!\n";
    }
}

# print all stages
sub printStages {
    my $self = shift;

    # TODO print the stages in some sensible order

    my @keys = keys %{ $self->{STAGES} };
    print "Number of stages = $#keys\n\n";
    foreach my $key ( @keys ) {
	$self->printStage($key);
    }
}

1; # so that require or use succeeds
