#!/usr/local/unstable/bin/perl -w

# run standard insect using the PMP system
# mostly meant as a test-bed for PMP

use strict;
use PMPpbs;
use PMParray;
use PMP;
use MNI::Startup;
use MNI::PathUtilities qw(split_path);
use MNI::FileUtilities qw(check_output_dirs check_output_path);
use MNI::DataDir;
use Getopt::Tabular;

# globals
my $clsDir = MNI::DataDir::dir("ICBM");
my $model = "$clsDir/icbm_template_1.00mm.mnc";

# arguments and their defaults
my $usePBS = undef;
my $pbsQueue = "long";
my $pbsHosts = "yorick:bullcalf";
my $command = "printStatus";

my @leftOverArgs;

# argument handling
my @argTbl = (
    ["Pipeline options", "section"],
    ["-pbs", "boolean", undef, \$usePBS,
     "Use PBS for job submission."],
    ["-queue", "string", 1, \$pbsQueue,
     "Which PBS queue to use [short|medium|long]."],
    ["-hosts", "string", 1, \$pbsHosts,
     "Colon separated list of pbs hosts"],
    ["Pipeline control", "section"],
    ["-run", "const", "run", \$command,
     "Run the pipeline."],
    ["-status-from-files", "const", "statusFromFiles", \$command,
     "Compute pipeline status from files"],
    ["-print-stages", "const", "printStages", \$command,
     "Print the pipeline stages."],
    ["-print-status", "const", "printStatus", \$command,
     "Print the status of each pipeline."]
);
GetOptions(\@argTbl, \@ARGV, \@leftOverArgs) or die "\n";

my $usage = "$ProgramName basedir nativet1_1.mnc nativet1_n.mnc\n";
my $pipebase = shift @leftOverArgs or die $usage;
my @filenames = @leftOverArgs or die $usage;

# an array to store the pipeline definitions for each subject
my $pipes = PMParray->new();

# set up the pipeline for each subject
foreach my $filename (@filenames) {

    # set the name based on splitting the input filename
    my ($dir, $base, $ext) = split_path($filename, 'last', [qw(gz z Z)]);
    my $basedir = "${pipebase}/${base}";
    my $logdir = "${basedir}/logs";

    # create the output directories if necessary
    system("mkdir -p $logdir") unless ( -d $logdir );
    
    # filename definitions
    my $nuc = "${basedir}/${base}_nuc.mnc";
    my $talTransform = "${basedir}/${base}_total.xfm";
    my $final = "${basedir}/${base}_final.mnc";
    my $cls = "${basedir}/${base}_cls.mnc";

    # define the pipeline
    my $pipeline;
    if ($usePBS) {
	# use parallel execution
	$pipeline = PMPpbs->new();
	$pipeline->setQueue($pbsQueue);
	$pipeline->setHosts($pbsHosts);
    }
    else {
	# use sequential execution
	$pipeline = PMP->new();
    }

    $pipeline->name("insect-${base}");
    $pipeline->statusDir($logdir);

    # add the various stages
    $pipeline->addStage(
	{ name => "nuc",
	  inputs => [$filename],
	  outputs => [$nuc],
	  args => ["nu_correct", $filename, $nuc] });
    $pipeline->addStage(
	{ name => "total",
	  inputs => [$filename],
	  outputs => [$talTransform],
	  args => ["mritotal", $filename, $talTransform] });
    $pipeline->addStage(
	{ name => "final",
	  inputs => [$talTransform, $nuc],
	  outputs => [$final],
	  args => ["mincresample", "-like", $model, "-transform", 
		   $talTransform, $nuc, $final],
	  prereqs => ["nuc", "total"] });
    $pipeline->addStage(
	{ name => "cls",
	  inputs => [$final],
	  outputs => [$cls],
	  args => ["classify_clean", "-clobber", "-clean_tags", $final, $cls],
	  prereqs => ["final"] });
    
    # rerun any failures from a previous run of this subjects pipe
    $pipeline->resetFailures();
    # assume that files considered to be running are faulty
    $pipeline->resetRunning();
    # compute the status of each stage
    $pipeline->updateStatus();

    # now add this pipe to our happy array of pipes
    $pipes->addPipe($pipeline);
}

# now run whatever it is that the user wanted done
if ($command eq "printStatus" ) {
    $pipes->printUnfinished();
}
elsif ($command eq "statusFromFiles") {
    $pipes->updateFromFiles();
}
elsif ($command eq "printStages") {
    $pipes->printStages();
}
elsif ($command eq "run") { 
    $pipes->run();
}
else {
    print "huh? Grunkle little gnu, grunkle\n";
}




