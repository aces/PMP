#!/usr/local/unstable/bin/perl -w

# run standard insect using the PMP system
# mostly meant as a test-bed for PMP

use strict;
use PMPpbs;
use MNI::Startup;
use MNI::PathUtilities qw(split_path);
use MNI::FileUtilities qw(check_output_dirs check_output_path);
use MNI::DataDir;
use Getopt::Tabular;

# globals
my $clsDir = MNI::DataDir::dir("ICBM");
my $model = "$clsDir/icbm_template_1.00mm.mnc";

my $usage = "$ProgramName basedir nativet1_1.mnc nativet1_n.mnc\n";
my $pipebase = shift @ARGV or die $usage;
my @filenames = @ARGV or die $usage;

# an array to store the pipeline definitions for each subject
my @pipes;

# set up the pipeline for each subject
foreach my $filename (@filenames) {

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
    my $pipeline = PMPpbs->new();
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

    # print the various stages to stdout
    $pipeline->printStages();
    # compute the status of each stage
    $pipeline->updateStatus();

    # now add this pipe to our happy array of pipes
    push @pipes, $pipeline;
}

# now run the pipelines
my $allFinished = 0;
while (! $allFinished) {
    $allFinished = 1;
    foreach my $pipeline (@pipes) {
	my $status = $pipeline->run();
	# if a single pipeline still has stages left set allFinished to 0
	$allFinished = 0 if $status;
	print "Status: $status - finished: $allFinished\n";
    }
    print "F: $allFinished\n";
    sleep 5;
}

print "Pipelines finished!\n";



