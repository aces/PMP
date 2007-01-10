###
# Package PMP - Poor Man's Pipeline
#
# Some basic pipelining that is elegant to use and not dependent on
# too many modules or databases.

package PMP::PMP;
use strict;
use MNI::Spawn;
use MNI::Startup;
use MNI::MiscUtilities qw(shellquote timestamp);

# the version number

$PMP::VERSION = '0.7.0';

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
    # lock flag on pipeline (-1=not initialized; 0=done; >0=locked)
    $self->{lock} = -1;
    # holds a vaguely dependency tree like object;
    $self->{dependencyTree} = [];
    # whether all stages or only a subset are to be run
    $self->{runAllStages} = 1;
    # a list of the subset of stages that are to be run
    $self->{stageSubset} = {};

    # set the spawning options
    # MNI::Spawn::SetOptions( err_action => 'ignore' );

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

    # shell quoting: by default, shell quoting is turned off as it can
    # cause problems of its own. It can be turned on with a special
    # field (which is optional).
    if (! exists $$stage{'shellquote'} ) {
	$$stage{'shellquote'} = 0;
    }

    # the stage definition has passed the various tests - now add some 
    # internal record keeping elements to it.

    $$stage{'order'} = $self->{CURRENT};
    $self->{CURRENT}++;
    $$stage{'finished'} = 0;
    $$stage{'failed'} = 0;
    $$stage{'running'} = 0;
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
    print "Pipe $self->{NAME}: unfinished stages: \n";
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
	my $finished = $self->stageStatusFromFiles($stage);
        if( $finished && ( -f $self->getFinishedFile($stage) ) ) {
          $self->{STAGES}{$stage}{'finished'} = 1;
        }
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

    foreach my $stage (@{ $self->{STAGES} }) {
        # dot doesn't like dashes or periods
        my $dest = $stage;
        $dest =~ s/-/_/g;
	$dest =~ s/\./_/g;
        
        print DOT "$dest [shape=Mrecord, label=\"{$stage|";
        foreach my $input ( @{ $self->{STAGES}{$stage}{'outputs'} } ) {
            $input =~ s/$substring//g if $substring;
            print DOT "$input\\n";
        }
        print DOT "}\"]\n";
        
        foreach my $dep ( @{ $self->{STAGES}{$stage}{'prereqs'} } ) {
            my $source = $dep;
            # dot doesn't like dashes or periods
            $source =~ s/-/_/g;
	    $source =~ s/\./_/g;
            print DOT "$source -> ${dest};\n";
        }
    }

    print DOT "}\n";
    close DOT;
}

# create a file that can be used by dot to generate a dependency graph
sub createDotGraph {
    my $self = shift;
    my $filename = shift;

    #$self->sortStages() unless $self->{isSorted};

    open DOT, ">$filename" or die "ERROR opening dot file $filename: $!\n";

    print DOT "digraph G {\n";

    foreach my $stage (keys %{ $self->{STAGES} }) {
	# dot doesn't like dashes or periods
	my $dest = $stage;
	$dest =~ s/-/_/g;
	$dest =~ s/\./_/g;

	# add a descriptive label if it exists
	if ( exists $self->{STAGES}{$stage}{'label'} ) {
	    print DOT "$dest [shape=Mrecord, " .
		"label=\"{$stage|$self->{STAGES}{$stage}{'label'}}\"]\n";
	}
	else {
	    # keep the original label
	    print DOT "$dest [label=$stage]\n";
	}


	foreach my $dep ( @{ $self->{STAGES}{$stage}{'prereqs'} } ) {
	    my $source = $dep;
	    # dot doesn't like dashes or periods
	    $source =~ s/-/_/g;
	    $source =~ s/\./_/g;
	    print DOT "$source -> ${dest};\n";
	}
    }
    print DOT "}\n";
    close DOT;
}

# gets the pipeline's status. Messages come in these flavours:
#   running (including list of stages currently running)
#   queued  (including list of stages that are submitted but not yet running)
#   failed  (including list of stages that have failed)
#   finished (including list of stages that are finished)

sub getPipelineStatus {
  my $self = shift;

  # make sure that stages are sorted
  $self->sortStages() unless $self->{isSorted};

  # variables
  my @runningStages;
  my @queuedStages;
  my @failedStages;
  my @finishedStages;

  # sort each stage into one of the four possible arrays
  foreach my $stage ( @{ $self->{sortedStages} } ) {
    if ($self->{STAGES}{$stage}{failed}) {
      push @failedStages, $stage;
    } elsif ($self->{STAGES}{$stage}{finished}) {
      push @finishedStages, $stage;
    } elsif ($self->{STAGES}{$stage}{running}) {
      if( -f $self->getLogFile($stage) ) {
        push @runningStages, $stage;
      } else {
        push @queuedStages, $stage;
      }
    }
  }

  my $overallStatus = "Pipe $self->{NAME} status: \n";

  if (@failedStages) {
    $overallStatus .= "  Failed: @failedStages\n";
  }
  if (@finishedStages) {
    $overallStatus .= "  Finished: @finishedStages\n";
  }
  if (@runningStages) {
    $overallStatus .= "  Running: @runningStages\n";
  }
  if (@queuedStages) {
    $overallStatus .= "  Queued: @queuedStages\n";
  }
  return $overallStatus;
}

# Returns the numbre of jobs currently queued 
# This is a rather brute force method; would be better to keep track
# of queued stages explicitly

sub nQueued {
  my $self = shift;

  # make sure stages are sorted
  $self->sortStages() unless $self->{isSorted};
  
  my $nQueued = 0;
  foreach my $stage ( @{ $self->{sortedStages} } ) {
      if ($self->{STAGES}{$stage}{running} &&
	  (! -f $self->getLogFile($stage))) {
	  $nQueued++;
      }
  }
  
  return $nQueued;
}

# prints, in CSV format, the header row for a status report. Takes a
# filehandle reference as the argument.
sub printStatusReportHeader {
    my $self = shift;
    my $fileHandle = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    # print the ID as the first element
    print $fileHandle "ID," or die
	"ERROR writing to file in printStatusReportHeader: $!\n";

    # now print the individual stage names
    foreach my $stage ( @{ $self->{sortedStages} } ) {
	print $fileHandle "${stage}," or die
	    "ERROR writing to file in printStatusReportHeader: $!\n";
    }
    print $fileHandle "\n";
}

# prints the status of each stage to a CSV file. Takes a filehandle as
# an argument
sub printStatusReport {
    my $self = shift;
    my $fileHandle = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    # print the stage name
    print $fileHandle "$self->{NAME}," or die
	"ERROR writing to file in printStatusReport: $!\n";

    # print the status
    foreach my $stage ( @{ $self->{sortedStages} } ) {
	if ( $self->{STAGES}{$stage}{failed}) {
	    print $fileHandle "failed,";
	}
	elsif ($self->{STAGES}{$stage}{finished}) {
	    print $fileHandle "finished,";
	}
	elsif ($self->{STAGES}{$stage}{running}) {
            if( -f $self->getLogFile($stage) ) {
	        print $fileHandle "running,";
            } else {
	        print $fileHandle "queued,";
            }
	}
	else {
	    print $fileHandle "not started,";
	}
    }
    print $fileHandle "\n";
}

# compute the stage dependencies based in input and outputs
sub computeDependenciesFromInputs {
  my $self = shift;
  
  # create a hash of all outputs mapped to their stages
  # note: requires outputs to be unique
  my %output_stages;
  foreach my $key (keys %{ $self->{STAGES} }) {
    foreach my $output ( @{$self->{STAGES}{$key}{'outputs'}} ) {
      $output_stages{$output} =  $self->{STAGES}{$key}{'name'};
    }
  }

  # now traverse stages and set inputs
  foreach my $key (keys %{ $self->{STAGES} }) {
    foreach my $input ( @{$self->{STAGES}{$key}{'inputs'}} ) {
      # check to see if input is in output list
      if ($output_stages{$input}) {
	# stage is in output list, so push onto prereqs
	print "pushing $output_stages{$input} onto $self->{STAGES}{$key}{'name'}\n";
	push @{$self->{STAGES}{$key}{'prereqs'}}, $output_stages{$input};
      }
    }
  }
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
	if (! exists $self->{STAGES}{$key}{'prereqs'} or (! defined $self->{STAGES}{$key}{'prereqs'}[0])) {
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
	    foreach my $stage ( @{ $self->{STAGES}{$key}{'prereqs'} } ) {
		print "Prereq $stage of $key\n" if $self->{DEBUG};
		print "return of grep: " . grep(/$stage/, @insertedStages) 
		    if $self->{DEBUG};
		if (defined $stage and ! grep( /^$stage$/, @insertedStages ) and
		    grep( /^$stage$/, @keys) ) {
		    print "prereq $stage for $key not in list\n" 
			if $self->{DEBUG};
		    $validInsertion = 0;
		    last;
		}
		elsif (! grep( /$stage/, @keys) ) {
		    warn "Prereq: $stage of stage $key does not exist - ignoring\n";
		}
	    }
	    if ($validInsertion == 1) { 
		push @insertedStages, $key;
		push @{ $dependencyTree[$insertionLevel] }, $key;
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
    # only beast requiring MNI::Spawn in this file
    RegisterPrograms(\@programs) or die "ERROR: could not register programs: $!\n";
    $self->{areRegistered} = 1;
}

# Initialize the lock file for this pipeline.
sub initLockFile {

    my $self = shift;

    my $lockfile = $self->getLockFile();
    if (! -e $lockfile) {
      $self->{lock} = 1;
      system( "touch $lockfile" );
      print "Lock file for pipe $self->{NAME} created.\n";
    } else {
      $self->{lock} = 0;
      print "Lock file for pipe $self->{NAME} already exists.\n";
    }
}

# Remove the lock file for this pipeline.
sub cleanLockFile {

    my $self = shift;

    if( $self->{lock} ) {
      my $lockfile = $self->getLockFile();
      if ( -e $lockfile) {
        print "Lock file for pipe $self->{NAME} removed.\n";
        unlink $lockfile;
      }
      $self->{lock} = 0;
    }
}

# run the next iteration
sub run {
    my $self = shift;

    # make sure the programs have all been registered
    $self->registerPrograms() unless $self->{areRegistered};

    # initialize the lock file for this pipeline.
    if( $self->{lock} == -1 ) {
      $self->initLockFile() 
    }

    my $status = 0;

    if( $self->{lock} ) {    
      foreach my $key ( @{ $self->{toBeExecuted} } ) {
	$self->execStage($key);
      }
      $status = $self->updateStatus();
      if( $status == 0 ) {
        $self->cleanLockFile() 
      }
    }

    return $status;
}

# Get stage status
sub stageStatus {
    my $self = shift;
    my $stageName = shift;
    my $status = shift;

    my $returnVal = 0;

    # check whether the status flag is set
    if ( $self->{STAGES}{$stageName}{$status} ) { 
	$returnVal = 1;
    }
    elsif ( -f $self->getStageFile($stageName, $status) ) {
	# We have a status file but the status in the hash was not set
	# to finished
	printf "[%s] Changing status of %s in pipe %s to %s\n", 
		timestamp(), $stageName, $self->{NAME}, $status;
	$self->{STAGES}{$stageName}{$status} = 1;
	$returnVal =  1;
    }
    return $returnVal;
}

# query whether a stage is finished
sub isStageFinished {
    my $self = shift;
    my $stageName = shift;

    return $self->stageStatus($stageName, 'finished');
}

# query whether a stage is running
sub isStageRunning {
    my $self = shift;
    my $stageName = shift;

    return $self->stageStatus($stageName, 'running');
}

# query whether a stage has failed
sub isStageFailed {
    my $self = shift;
    my $stageName = shift;

    return $self->stageStatus($stageName, 'failed');
}

# reset the status of a stage to make it runnable
sub resetStage {
    my $self = shift;
    my $stageName = shift;

    # The following statuses should be exclusive, but
    # sometimes there can be a mess when there is a 
    # crash (.running file not deleted, for example).
    # So be mean and wipe out everything. Don't ask
    # questions. Remove .log, .finished, .failed, 
    # .running on a stage reset.

    if( -f $self->getLogFile($stageName) ) {
	unlink $self->getLogFile($stageName);
    }
    if( -f $self->getFinishedFile($stageName) ) {
	unlink $self->getFinishedFile($stageName);
    }
    if( -f $self->getFailedFile($stageName) ) {
	unlink $self->getFailedFile($stageName);
    }
    if( -f $self->getRunningFile($stageName) ) {
	unlink $self->getRunningFile($stageName);
    }

    $self->{STAGES}{$stageName}{'failed'} = 0;
    $self->{STAGES}{$stageName}{'finished'} = 0;
    $self->{STAGES}{$stageName}{'running'} = 0;
}

# allow all failures to be rerun
# Extend the concept of failure to include:
#    - stage has failed
#    - stage is apparently running but has finished (kill it!)
#
sub resetFailures {
    my $self = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    foreach my $key (@{ $self->{sortedStages} }) {
        if( $self->isStageFailed($key) || 
            ( $self->isStageFinished($key) && $self->isStageRunning($key) ) ) {
            print "Pipe $self->{NAME}: Failed stage $key\n";
	    $self->resetFromStage($key);
        }
    }

    # Some validation on the prereqs (this does not really belong here but
    # it does the job anyway).
    my $prereq_error = 0;
    foreach my $stage (@{ $self->{sortedStages} }) {
	foreach my $key ( @{ $self->{STAGES}{$stage}{'prereqs'} } ) {
            if ( !grep(/$key/, @{$self->{sortedStages}}) ) {
                print "Pipe $self->{NAME}: Prereq $key of stage $stage is not a valid pipeline stage.\n";
                $prereq_error++;
            }
        }
    }
    if( $prereq_error ) {
        die "Sorry dude, I must quit.\n";
    }

    # Reset all finished stages which depend on an unfinished/failed stage as a prereq.
    foreach my $stage (@{ $self->{sortedStages} }) {
        if ( $self->isStageFinished($stage) ) {
            my $ready = 1;
	    # check if this stage is really finished based on its prereqs
	    foreach my $key ( @{ $self->{STAGES}{$stage}{'prereqs'} } ) {
                if ( !$self->isStageFinished($key) ) {
                    print "Pipe $self->{NAME}: Prereq $key of stage $stage is not finished.\n";
		    $ready = 0;
                }
	    }
            if( !$ready ) {
	        $self->resetStage($stage);
            }
        }
    }

}

# creates a subset of stages up to a specified end-point
sub subsetToStage {
    my $self = shift;
    my $stageName = shift;

    # make sure stages are sorted
    $self->sortStages() unless $self->{isSorted};

    # set a flag indicating that subsets are in use
    $self->{runAllStages} = 0;

    foreach my $stage (@{ $self->{sortedStages} }) {
	$self->{stagesSubset}{$stage} = 1;
	print "ADDING TO SUBSET: $stage\n";
	if ($stage eq $stageName) { last; }
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

    print "Pipe $self->{NAME}: resetting all stages from $stageName\n";

    while ($numAdded) { #keep going until no more stages are added
	$numAdded = 0;
	foreach my $stage ( @{ $self->{sortedStages} } ) {
	    foreach my $prereq ( @stagesToBeReset ) {
		if (grep(/^$prereq$/, @{ $self->{STAGES}{$stage}{'prereqs'} })) {
		    unless (grep(/^$stage$/, @stagesToBeReset)) {
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
        if( ( -f $self->getLogFile($stage) ) or
            ( -f $self->getFinishedFile($stage) ) or
            ( -f $self->getFailedFile($stage) ) or
            ( -f $self->getRunningFile($stage) ) ) {
          print "Pipe $self->{NAME}: reset $stage\n";
        }
	$self->resetStage($stage);
    }
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
#   - look at files .failed, .finished, .running to reset
#     the flags
#   - determine if the stage is ready to run based on its
#     prereqs
# Note: execStage() called before this touches the files,
#       but does not update the internal flags after execution
#       (unknown for sge and pbs).
#
sub updateStageStatus {
    my $self = shift;
    my $stageName = shift;

    my $runnable = 0;

    # check to make sure that this stage is in the subset of stages to be run
    if ($self->{runAllStages} == 1 || $self->{stagesSubset}{$stageName} ) {

	$runnable = 1;

        # Check if job just terminated normally. Reset flag if done.
        # (file .running does not exist but running flag is still set,
        # and .failed or .finished exists)
        if ( $self->{STAGES}{$stageName}{'running'} &&
             ( ! -f $self->getRunningFile($stageName) ) && 
             ( -f $self->getFailedFile($stageName) ||
               -f $self->getFinishedFile($stageName) ) ) {
	    $runnable = 0;
	    $self->{STAGES}{$stageName}{'running'} = 0;

            # Make sure all outputs have been created for a finished stage.
            if( -f $self->getFinishedFile($stageName) ) {
                foreach my $file (@{ $self->{STAGES}{$stageName}{'inputs'} }) {
	            if (! -e $file) {
                        print "Stage $stageName finished but output $file has not been created.\n";
                        $self->printStage($stageName);
                        print "Check your configuration for stage inputs and prereqs.\n";
                        die "Sorry dude, I must quit.\n";
                    }
                }
            }
        }

	# check to make sure that it has neither finished nor failed
	if ( $self->isStageFailed($stageName) ) {
            $runnable = 0;
	    $self->{STAGES}{$stageName}{'running'} = 0;
	    $self->{STAGES}{$stageName}{'finished'} = 0;
        } elsif ( $self->isStageFinished($stageName) ) {
            $runnable = 0;
	    $self->{STAGES}{$stageName}{'running'} = 0;
	} elsif ( $self->isStageRunning($stageName) ) {
            # check whether a stage is running
	    $self->{haveRunningStages} = 1;
	    $runnable = 0;
	} elsif (! exists $self->{STAGES}{$stageName}{'prereqs'} or
                (!defined $self->{STAGES}{$stageName}{'prereqs'}[0] ) or
                $#{ $self->{STAGES}{$stageName}{'prereqs'}} == -1 ) {
	    # if a stage has no prereqs it is runnable
	    $runnable = 1;
	} else { 
	    # check if all the prereqs are finished 
	    foreach my $stage ( @{ $self->{STAGES}{$stageName}{'prereqs'} } ) {
                if (grep(/$stage/, @{$self->{sortedStages}}) and
                    $self->{STAGES}{$stage}{'finished'} == 0) {
		    $runnable = 0;
		    last;
		}
	    }
	}

        # Make sure all inputs exist if all prereqs are satisfied.
        if( $runnable ) {
            foreach my $file (@{ $self->{STAGES}{$stageName}{'inputs'} }) {
	        if (! -e $file) {
	            # input file does not exist
	            $runnable = 0;
                    print "A conflict has been found with the specification of the \n";
                    print "inputs and the prereqs in this stage:\n";
                    $self->printStage($stageName);
                    print "All prereqs are finished but input $file does not exist.\n";
                    die "Sorry dude, I must quit.\n";
	            last;
	        }
	    }
	    $self->{STAGES}{$stageName}{'runnable'} = $runnable;
        }

    }

    return $runnable;
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

sub getStageFile {
    my $self = shift;
    my $stageName = shift;
    my $status = shift;
    return $self->getStatusBase($stageName) . ".${status}";
}

sub getLockFile { 
    my $self = shift;
    return "$self->{STATUSDIR}/$self->{NAME}.lock";
}

# designate a stage as running
sub declareStageRunning {
    my $self = shift;
    my $stageName = shift;
    
    my $runningFile = $self->getRunningFile($stageName);
    system( "touch $runningFile" );
    $self->{STAGES}{$stageName}{'running'} = 1;
    printf "[%s] Changing status of %s in pipe %s to running\n", timestamp(), $stageName, $self->{NAME};
}

# designate a stage as having finished
sub declareStageFinished {
    my $self = shift;
    my $stageName = shift;
    
    my $finishedFile = $self->getFinishedFile($stageName);
    if( ! -f $finishedFile ) {
      system( "touch $finishedFile" );
    }

    my $runningFile = $self->getRunningFile($stageName);
    if( -f $runningFile ) {
      unlink $runningFile;
    }

    $self->{STAGES}{$stageName}{'running'} = 0;
    $self->{STAGES}{$stageName}{'finished'} = 1;
}

# designate a stage as having failed
sub declareStageFailed {
    my $self = shift;
    my $stageName = shift;

    my $failedFile = $self->getFailedFile($stageName);
    system( "touch $failedFile" );

    my $runningFile = $self->getRunningFile($stageName);
    if( -f $runningFile ) {
      unlink $runningFile;
    }

    $self->{STAGES}{$stageName}{'running'} = 0;
    $self->{STAGES}{$stageName}{'failed'} = 1;
}

# print the definition of a single stage
sub printStage {
    my $self = shift;
    my $stageName = shift;

    if (exists $self->{STAGES}{$stageName}) {
	my $stage = $self->{STAGES}{$stageName};
	my $cmdstring = "@{ $$stage{'args'} } ";
	$cmdstring = shellquote(@{ $$stage{'args'} }) 
	    if ($$stage{'shellquote'});
	print "======= $self->{NAME}: $$stage{'order'}: $stageName ========\n";
	print "Inputs: @{ $$stage{'inputs'} }\n";
	print "Outputs: @{ $$stage{'outputs'} }\n";
	print "Args: $cmdstring\n";
	print "Prereqs: @{ $$stage{'prereqs'} }\n" 
	    if exists $$stage{'prereqs'};
        print "Status: ";
        if( $$stage{'finished'} ) {
            print "finished\n";
        } elsif( $$stage{'failed'} ) {
            print "failed\n";
        } elsif( $$stage{'running'} ) {
            if( -f $self->getLogFile($stageName) ) {
	        print "running\n";
            } else {
	        print "queued\n";
            }
        } elsif( !( $self->{runAllStages} == 1 || $self->{stagesSubset}{$stageName} ) ) {
            print "not in subset\n";
        } else {
            print "not processed\n";
        }
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
