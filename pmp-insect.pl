#!/usr/bin/env perl

# run standard insect using the PMP system
# mostly meant as a test-bed for PMP

use strict;
use PMP::pbs;
use PMP::sge;
use PMP::Array;
use PMP::PMP;
use MNI::Startup;
use MNI::PathUtilities qw(split_path);
use MNI::FileUtilities qw(check_output_dirs check_output_path);
use MNI::DataDir;
use Getopt::Tabular;

# globals
my $clsDir = MNI::DataDir::dir("ICBM");
my $autoregDir = MNI::DataDir::dir("mni_autoreg");
my $regModel = "icbm_avg_152_t1_tal_lin_symmetric";
my $model = "$clsDir/icbm_template_1.00mm.mnc";

# arguments and their defaults
my $PMPtype = "spawn";
my $pbsQueue = "long";
my $pbsHosts = "yorick:bullcalf";
my $command = "printStatus";
my $reset = undef;
my $sleepTime = 10;
my $runPipeTests = undef;

my @leftOverArgs;

# argument handling
my @argTbl = (
    ["Execution control", "section"],
    ["-spawn", "const", "spawn", \$PMPtype,
     "Use the perl system interface to spawn jobs [default]"],
    ["-sge", "const", "sge", \$PMPtype,
     "Use SGE to spawn jobs"],
    ["-pbs", "const", "pbs", \$PMPtype,
     "Use PBS to spawn jobs"],
    ["-queue", "string", 1, \$pbsQueue,
     "Which PBS queue to use [short|medium|long]."],
    ["-hosts", "string", 1, \$pbsHosts,
     "Colon separated list of PBS hosts"],
    ["-sleep-time", "integer", 1, \$sleepTime,
     "Set the sleep time."],

    ["Pipeline control", "section"],
    ["-run", "const", "run", \$command,
     "Run the pipeline."],
    ["-status-from-files", "const", "statusFromFiles", \$command,
     "Compute pipeline status from files"],
    ["-print-stages", "const", "printStages", \$command,
     "Print the pipeline stages."],
    ["-print-status", "const", "printStatus", \$command,
     "Print the status of each pipeline."],
    ["-write-status-report", "const", "writeReport", \$command,
     "Write a CSV separated status report."],

    ["Stage Control", "section"],
    ["-reset-all", "const", "resetAll", \$reset,
     "Start the pipeline from the beginning."],
    ["-reset-from", "string", 1, \$reset,
     "Restart from the specified stage."],

    ["Pipeline Tests", "section"],
    ["-run-tests|-no-tests", "boolean", undef, \$runPipeTests,
     "Creates averages of all cls files for testing purposes"],
);
GetOptions(\@argTbl, \@ARGV, \@leftOverArgs) or die "\n";

my $usage = "$ProgramName basedir nativet1_1.mnc nativet1_n.mnc\n";
my $pipebase = shift @leftOverArgs or die $usage;
my @filenames = @leftOverArgs or die $usage;

# an array to store the pipeline definitions for each subject
my $pipes = PMP::Array->new();

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
    if ($PMPtype eq "pbs") {
      # use parallel execution
      $pipeline = PMP::pbs->new();
      $pipeline->setQueue($pbsQueue);
      $pipeline->setHosts($pbsHosts);
      $pipeline->setPriorityScheme("later-stages");

    }
    elsif ($PMPtype eq "sge") {
        $pipeline = PMP::sge->new();
    }
    elsif ($PMPtype eq "spawn") {
        $pipeline = PMP::PMP->new();
    }

    $pipeline->name("insect-${base}");
    $pipeline->statusDir($logdir);

    # turn off debug messaging for now
    $pipeline->debug(0);

    # add the various stages
    $pipeline->addStage(
	{ name => "nuc",
	  label => "Non Uniformity \\nCorrection",
	  inputs => [$filename],
	  outputs => [$nuc],
	  args => ["nu_correct", "-clobber", $filename, $nuc] });
    $pipeline->addStage(
	{ name => "total",
	  label => "stereotaxic registration",
	  inputs => [$filename],
	  outputs => [$talTransform],
	  args => ["mritotal", "-clobber", "-modeldir", $autoregDir, "-model", 
               $regModel, $filename, $talTransform] });
    $pipeline->addStage(
	{ name => "final",
	  label => "resampling",
	  inputs => [$talTransform, $nuc],
	  outputs => [$final],
	  args => ["mincresample", "-clobber", "-like", $model, "-transform", 
		   $talTransform, $nuc, $final],
	  prereqs => ["nuc", "total"] });
    $pipeline->addStage(
	{ name => "cls",
	  label => "classification",
	  inputs => [$final],
	  outputs => [$cls],
	  args => ["classify_clean", "-clobber", "-clean_tags", $final, $cls],
	  prereqs => ["final"] });
    
    # rerun any failures from a previous run of this subjects pipe
    $pipeline->resetFailures();
    # assume that files considered to be running are faulty
    #$pipeline->resetRunning();
    # compute the status of each stage
    $pipeline->updateStatus();
    
    # create a dependency graph that can be turned into a pretty picture.
    #$pipeline->createDotGraph("test.dot");

    # print the current pipe status
    print "Status: " . $pipeline->getPipelineStatus() . "\n";

    # now add this pipe to our happy array of pipes
    $pipes->addPipe($pipeline);
}

# if any stages were to be reset, do so now
if ($reset) {
    if ($reset eq "resetAll") {
	$pipes->resetAll();
    }
    else {
	$pipes->resetFromStage($reset);
    }
}

# now run whatever it is that the user wanted done
if ($command eq "printStatus" ) {
    $pipes->printUnfinished();
    $pipes->printPipelineStatus();

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
elsif ($command eq "writeReport") {
    $pipes->printStatusReport("status-report.csv");
}
else {
    print "huh? Grunkle little gnu, grunkle\n";
}

# ugly test code
if ($runPipeTests) {
    my $cls_files;
    foreach my $p ( @{ $pipes->{PIPES} }) {
        if (! $p->{STAGES}{cls}{finished} ) {
            die "ABORTING TESTS: at least one cls stage did not finish\n";
        }
        $cls_files .= $p->{STAGES}{cls}{outputs}[0];
        $cls_files .= " ";
    }
    #print "CLS FILES: $cls_files\n";
    #print "NUM PIPES: $#{$pipes->{PIPES}}\n";
    my $sdfile = "pipe-test-gm-sd.mnc";
    my $avgfile = "pipe-test-gm-avg.mnc";
    system("mincaverage -clobber -binarize -binvalue 2 -sdfile $sdfile $cls_files $avgfile");

}

