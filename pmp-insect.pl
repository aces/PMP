#!/usr/local/unstable/bin/perl -w

# run standard insect using the PMP system
# mostly meant as a test-bed for PMP

use strict;
use PMP;
use MNI::Startup;
use MNI::PathUtilities qw(split_path);
use MNI::FileUtilities qw(check_output_dirs check_output_path);
use MNI::DataDir;
use Getopt::Tabular;

my $usage = "$ProgramName nativet1.mnc basedir\n";
my $filename = shift @ARGV or die $usage;
my $basedir = shift @ARGV or die $usage;
my $logdir = "${basedir}/logs";

check_output_dirs($logdir);

my ($dir, $base, $ext) = split_path($filename, 'last', [qw(gz z Z)]);

# globals
my $clsDir = MNI::DataDir::dir("ICBM");
my $model = "$clsDir/icbm_template_1.00mm.mnc";

# filename definitions
my $nuc = "${basedir}/${base}_nuc.mnc";
my $talTransform = "${basedir}/${base}_total.xfm";
my $final = "${basedir}/${base}_final.mnc";
my $cls = "${basedir}/${base}_cls.mnc";

# define the pipeline
my $pipeline = PMP->new();
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
      args => ["mincresample", "-like", $model, "-transform", $talTransform,
	       $nuc, $final],
      prereqs => ["nuc", "total"] });
$pipeline->addStage(
    { name => "cls",
      inputs => [$final],
      outputs => [$cls],
      args => ["classify_clean", "-clobber", "-clean_tags", $final, $cls],
      prereqs => ["final"] });

$pipeline->resetFailures();
$pipeline->printStages();
$pipeline->updateStatus();

while ($pipeline->run()) { }


