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
    # whether the programs have been registered yet.
    $self->{areRegistered} = 0;
    # list of stages to be executed in the next iteration
    $self->{toBeExecuted} = [];

    # set the spawning options
    MNI::Spawn::SetOptions( err_action => 'ignore' );

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
    # and that there is now an unregistered program
    $self->{isSorted} = 0;
    $self->{areRegistered} = 0;
    
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

# register the programs
sub registerPrograms {
    my $self = shift;

    my @programs;
    # touch is used to update stage result files
    push @programs, "touch"; 
    # the first element of the args part of each stage should be the program;
    foreach my $key ( keys %{ $self->{STAGES} } ) {
	push @programs, $self->{STAGES}{$key}{'args'}[0];
    }
    RegisterPrograms(\@programs) or die "ERROR: could not register programs: $!\n";
    $self->{areRegistered} = 1;
}

# run the next iteration
sub run {
    my $self = shift;

    # make sure the programs have all been registered
    $self->registerPrograms() unless $self->{areRegistered};
    
    foreach my $key ( @{ $self->{toBeExecuted} } ) {
	$self->execStage($key);
    }
    return $self->updateStatus();
}

# query whether a stage is finished
sub isStageFinished {
    my $self = shift;
    my $stageName = shift;

    my $returnVal = 0;

    # check whether finished status flag is set
    if ( $self->{STAGES}{$stageName}{'finished'} ) { 
	$returnVal = 1;
    }
    elsif ( -f $self->getFinishedFile($stageName) ) {
	# we have a failed file by the status in the hash was not set to finished
	warn "Changing status of $stageName in pipe $self->{NAME} to finished\n";
	$self->{STAGES}{$stageName}{'finished'} = 1;
	$returnVal =  1;
    }
    return $returnVal;
}

# query whether a stage has failed
sub isStageFailed {
    my $self = shift;
    my $stageName = shift;

    my $returnVal = 0;

    # check whether failed status flag is set
    if ( $self->{STAGES}{$stageName}{'failed'} ) { 
	$returnVal = 1;
    }
    elsif ( -f $self->getFailedFile($stageName) ) {
	# we have a failed file by the status in the hash was not set to fail
	warn "Changing status of $stageName in pipe $self->{NAME} to failed\n";
	$self->{STAGES}{$stageName}{'failed'} = 1;
	$returnVal =  1;
    }
    return $returnVal;
}

# reset the status of a stage to make it runnable
sub resetStage {
    my $self = shift;
    my $stageName = shift;

    if ( $self->isStageFinished($stageName) ) {
	unlink $self->getFinishedFile($stageName);
    }
    elsif ( $self->isStageFailed($stageName) ) {
	unlink $self->getFailedFile($stageName);
    }

    $self->{STAGES}{$stageName}{'failed'} = 0;
    $self->{STAGES}{$stageName}{'finished'} = 0;
    $self->{STAGES}{$stageName}{'running'} = 0;
}

# allow all failures to be rerun
sub resetFailures {
    my $self = shift;

    foreach my $key ( keys %{ $self->{STAGES} } ) {
	$self->resetStage($key) if $self->isStageFailed($key);
    }
}

# update the status of a stage
sub updateStageStatus {
    my $self = shift;
    my $stageName = shift;

    my $runnable = 1;

    # check to make sure that it has neither finished nor failed
    if ( $self->isStageFinished($stageName) || 
	 $self->isStageFailed($stageName) ) {
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

     # run the stage in question
    $self->declareStageRunning($stageName);
    my $status = Spawn($self->{STAGES}{$stageName}{'args'}, 
		       stdout => $self->getLogFile($stageName));
    if ($status != 0) {
	$self->declareStageFailed($stageName);
    }
    else {
	$self->declareStageFinished($stageName);
    }
}

# get the base filename for the various status files
sub getStatusBase {
    my $self = shift;
    my $stageName = shift;
    return "$self->{STATUSDIR}/$self->{NAME}.${stageName}";
}

sub getRunningFile {
    my $self = shift;
    my $stageName = shift;
    return $self->getStatusBase($stageName) . ".running";
}

sub getFailedFile {
    my $self = shift;
    my $stageName = shift;
    return $self->getStatusBase($stageName) . ".failed";
}

sub getFinishedFile {
    my $self = shift;
    my $stageName = shift;
    return $self->getStatusBase($stageName) . ".finished";
}

sub getLogFile { 
    my $self = shift;
    my $stageName = shift;
    return $self->getStatusBase($stageName) . ".log";
}

# designate a stage has running
sub declareStageRunning {
    my $self = shift;
    my $stageName = shift;
    
    my $runningFile = $self->getRunningFile($stageName);
    Spawn(["touch", $runningFile]);
    $self->{STAGES}{$stageName}{'running'} = 1;
}

# designate a stage as having finished
sub declareStageFinished {
    my $self = shift;
    my $stageName = shift;
    
    my $finishedFile = $self->getFinishedFile($stageName);
    my $runningFile = $self->getRunningFile($stageName);
    Spawn(["touch", $finishedFile]);
    unlink $runningFile;
    $self->{STAGES}{$stageName}{'running'} = 0;
    $self->{STAGES}{$stageName}{'finished'} = 1;
}

# designate a stage as having failed
sub declareStageFailed {
    my $self = shift;
    my $stageName = shift;

    my $failedFile = $self->getFailedFile($stageName);
    my $runningFile = $self->getRunningFile($stageName);
    Spawn(["touch", $failedFile]);
    unlink $runningFile;
    $self->{STAGES}{$stageName}{'running'} = 0;
    $self->{STAGES}{$stageName}{'failed'} = 1;
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

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    print "Number of stages = $#{ $self->{sortedStages} }\n\n";
    foreach my $key ( @{ $self->{sortedStages} } ) {
	$self->printStage($key);
    }
}

1; # so that require or use succeeds
