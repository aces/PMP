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
    # turns extra verbose printing on or off
    $self->{DEBUG} = 1;
    # holds a vaguely dependency tree like object;
    $self->{dependencyTree} = [];

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

# get or set the debug status
sub debug {
    my $self = shift;
    if (@_) { $self->{DEBUG} = shift; }
    return $self->{DEBUG};
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

# print all unfinished stages
sub printUnfinished {
    my $verbose = 0;
    my $self = shift;

    # optional verbose argument
    if (@_) { 
	$verbose = shift;
    }

    $self->sortStages() unless $self->{isSorted};
    print "$self->{NAME}: unfinished stages: \n";
    foreach my $stage ( @{ $self->{sortedStages} } ) {
	if (! $self->{STAGES}{$stage}{'finished'} ) {
	    if ($verbose) { $self->printStage($stage); }
	    else { print "$stage\n"; }
	}
    }
    print "\n";
}
   


# set the stage status from files for all stages
sub statusFromFiles {
    my $self = shift;

    foreach my $stage ( keys %{ $self->{STAGES} } ) {
	$self->stageStatusFromFiles($stage);
    }
}

# determine whether a stage is runnable based on filename existence/dates
sub stageStatusFromFiles {
    my $self = shift;
    my $stageName = shift;

    my $t = time();
    my $finished = 1;

    foreach my $file (@{ $self->{STAGES}{$stageName}{'outputs'} }) {
	if (-f $file) {
	    # file exists - get the oldest output file
	    my @stats = stat($file);
	    if ($stats[9] < $t) {
		$t = $stats[9];
	    }
	}
	else {
	    # output file does not exist
	    $finished = 0;
	}
    }

    # see if any of the input files are newer than the output files
    foreach my $file (@{ $self->{STAGES}{$stageName}{'inputs'} }) {
	if (! -e $file) {
	    # input file does not exist
	    $finished = 0;
	    last;
	}
	if ($finished) {
	    my @stats = stat($file);
	    if ($stats[9] > $t) {
		$finished = 0;
	    }
	}
    }
    $self->declareStageFinished($stageName) if $finished;
    return $finished;
}

# create a dependency graph of the filenames
# optional third argument - substring to remove
sub createFilenameDotGraph {
    my $self = shift;
    my $filename = shift;
    my $substring = undef;
    if (@_) { $substring = shift; }

    $self->sortStages() unless $self->{isSorted};

    open DOT, ">$filename" or die "ERROR opening dot file $filename: $!\n";

    print DOT "digraph G {\n";

    foreach my $stage (@{ $self->{sortedStages} }) {
	foreach my $out ( @{ $self->{STAGES}{$stage}{'outputs'} } ) {
	    foreach my $in ( @{ $self->{STAGES}{$stage}{'inputs'} } ) {
		my $source = $in;
		my $dest = $out;
		$source =~ s/$substring//g if $substring;
		$dest =~ s/$substring//g if $substring;
		$source =~ s/[\/\.-]/_/g;
		$dest =~ s/[\/\.-]/_/g;
		print DOT "$source -> {$dest};\n";
	    }
	}
    }
    print DOT "}\n";
    close DOT;
}

# create a file that can be used by dot to generate a dependency graph
sub createDotGraph {
    my $self = shift;
    my $filename = shift;

    $self->sortStages() unless $self->{isSorted};

    open DOT, ">$filename" or die "ERROR opening dot file $filename: $!\n";

    print DOT "digraph G {\n";

    foreach my $stage (@{ $self->{sortedStages} }) {
	foreach my $dep ( @{ $self->{STAGES}{$stage}{'prereqs'} } ) {
	    my $source = $dep;
	    # dot doesn't like dashes.
	    $source =~ s/-/_/g;
	    my $dest = $stage;
	    $dest =~ s/-/_/g;
	    print DOT "$source -> ${dest};\n";
	}
    }
    print DOT "}\n";
    close DOT;
}



# print the dependency tree
sub printDependencyTree {
    my $self = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    print "NOTE: this tree uses downwards and rightwards inheritance\n";
    foreach my $level (@{ $self->{dependencyTree} }) {
	print "@{$level} \n";
    }
    print "\n";
}

# created a sorted list of the various stages
sub sortStages {
    my $self = shift;

    # reinitialize the array to empty
    delete $self->{sortedStages};
    delete $self->{dependencyTree};

    my @keys = keys %{ $self->{STAGES} };
    my @insertedStages;
    my @uninsertedStages;
    my @dependencyTree;
    my $currentInsertion = 0;
    my $insertionLevel = 0;

    # stage 1: find all the stages with no prereqs at all
    foreach my $key ( @keys ) {
	if (! exists $self->{STAGES}{$key}{'prereqs'} ) {
	    push @insertedStages, $key;
	    push @{ $dependencyTree[$insertionLevel] }, $key;
	    $currentInsertion++;
	}
	else {
	    push @uninsertedStages, $key;
	}
    }

    # stage 2: insert stages which only depend on already inserted stages
    while ($#insertedStages < $#keys) {
	my @tmp; # temporarily hold uniserteable stages
	$insertionLevel++;
	foreach my $key ( @uninsertedStages ) {
	    my $validInsertion = 1;
	    #print "Now in stage $key\n";
	    foreach my $stage ( @{ $self->{STAGES}{$key}{'prereqs'} } ) {
		#print "Prereq $stage of $key\n";
		#print "blah: " . grep(/$stage/, @insertedStages);
		if (! grep( /$stage/, @insertedStages ) ) {
		    #print "prereq not in list\n";
		    $validInsertion = 0;
		    last;
		}
	    }
	    if ($validInsertion == 1) { 
		push @insertedStages, $key;
		push @{ $dependencyTree[$insertionLevel] }, $key;
		#print "inserted: $key\n";
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

    $self->{dependencyTree} = \@dependencyTree;
}
    
# update status of all stages
sub updateStatus {
    my $self = shift;
    delete $self->{toBeExecuted};
    my @toBeExecuted;

    # reset stages running flag
    $self->{haveRunningStages} = 0;

    # sort stages if necessary
    $self->sortStages() unless $self->{isSorted};

#    foreach my $key ( keys %{ $self->{STAGES} } ) {
    foreach my $key ( @{ $self->{sortedStages} } ) {
	if ($self->updateStageStatus($key)) {
	    push @toBeExecuted, $key;
	}
    }
    $self->{toBeExecuted} = \@toBeExecuted;

    my $numStagesToBeRun = $#toBeExecuted + 1;

    print "$numStagesToBeRun: stage[s] to be run: @toBeExecuted \n" 
	if $self->{DEBUG};
    # returns 0 if there are no more stages to be executed
    if ( $numStagesToBeRun ) {
	return $numStagesToBeRun;
    }
    elsif ( $self->{haveRunningStages} ) {
	return 1;
    }
    else {
	return 0;
    }
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
	print "Changing status of $stageName in pipe $self->{NAME} to finished\n";
	$self->{STAGES}{$stageName}{'finished'} = 1;
	$returnVal =  1;
    }
    return $returnVal;
}

# query whether a stage is running
sub isStageRunning {
    my $self = shift;
    my $stageName = shift;

    my $returnVal = 0;

    # check whether running status flag is set
    if ( $self->{STAGES}{$stageName}{'running'} ) { 
	$returnVal = 1;
    }
    elsif ( -f $self->getRunningFile($stageName) ) {
	# we have a running stage from the status files
	print "Changing status of $stageName in pipe $self->{NAME} to running\n";
	$self->{STAGES}{$stageName}{'running'} = 1;
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
	print "Changing status of $stageName in pipe $self->{NAME} to failed\n";
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
    elsif ( $self->isStageRunning($stageName) ) {
	unlink $self->getRunningFile($stageName);
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

# resets all stages from a certain point onwards
sub resetFromStage {
    my $self = shift;
    my $stageName = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    my @stagesToBeReset;
    push @stagesToBeReset, $stageName;

    my $numAdded = 1;

    print "\n$self->{NAME}: resetting all stages from $stageName\n";

    while ($numAdded) { #keep going until no more stages are added
	$numAdded = 0;
	foreach my $stage ( keys %{ $self->{STAGES} } ) {
	    foreach my $prereq ( @stagesToBeReset ) {
		if (grep(/$prereq/, @{ $self->{STAGES}{$stage}{'prereqs'} })) {
		    unless (grep(/$stage/, @stagesToBeReset)) {
			# a stage in the to be reset list is a prereq for this
			# stage - reset it too.
			push @stagesToBeReset, $stage; 
			$numAdded++;
		    }
		}
	    }
	}
    }

    # do the actual resetting
    foreach my $stage (@stagesToBeReset) {
	$self->resetStage($stage);
	print "$self->{NAME}: reset $stage\n";
    }
    print "\n";
}	    

# reset all stages
sub resetAll {
    my $self = shift;

    foreach my $key ( keys %{ $self->{STAGES} } ) {
	$self->resetStage($key);
    }
}

# rerun all current jobs that are running
sub resetRunning {
    my $self = shift;

    foreach my $key ( keys %{ $self->{STAGES} } ) {
	$self->resetStage($key) if $self->isStageRunning($key);
    }
}

# update the status of a stage
sub updateStageStatus {
    my $self = shift;
    my $stageName = shift;

    my $runnable = 1;

    print "Updating status of $self->{NAME} : $stageName\n" if $self->{DEBUG};

    # check to make sure that it has neither finished nor failed
    if ( $self->isStageFinished($stageName) || 
	 $self->isStageFailed($stageName) ) {
	print "Finished or failed\n" if $self->{DEBUG};
	$runnable = 0;
    }
    # check whether a stage is running
    elsif ( $self->isStageRunning($stageName) ) {
	print "Running\n" if $self->{DEBUG};
	$self->{haveRunningStages} = 1;
	$runnable = 0;
    }
    # if a stage has no prereqs it is runnable
    elsif (! exists $self->{STAGES}{$stageName}{'prereqs'} ) {
	print "Has no prereqs\n" if $self->{DEBUG};
	$self->{STAGES}{$stageName}{'runnable'} = 1;
    }
    # same if all the prereqs are finished 
    else { 
	foreach my $stage ( @{ $self->{STAGES}{$stageName}{'prereqs'} } ) {
	    if ($self->{STAGES}{$stage}{'finished'} == 0) {
		print "Prereq not finished\n" if $self->{DEBUG};
		$runnable = 0;
		last;
	    }
	}
	$self->{STAGES}{$stageName}{'runnable'} = $runnable;
    }
    print "Runnable status: $runnable\n\n" if $self->{DEBUG};
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
	
	print "======= $self->{NAME}: $$stage{'order'}: $stageName ========\n";
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
