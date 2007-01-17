#!/usr/bin/env perl -w

use strict;
use PMP::PMP;
use PMP::sge;

use Test::More;
# test if SGE's qsub is installed at all
my $sge_message = `qsub -help`;

# SGE needs a disk mounted across all systems executing jobs. Since this
# is unlikely to be standard, force it to be in an environment variable.
my $pipeDir = $ENV{PMP_SGE_TESTDIR};
if (! $sge_message =~ /SGE/) {
  plan skip_all => "SGE not installed - skipping tests.";
} 
elsif (! $pipeDir) {
  plan skip_all => "Environment variable PMP_SGE_TESTDIR not defined - skipping.";
}
elsif (! -w $pipeDir) {
  plan skip_all => "Cannot write to $pipeDir - skipping.";
}
else {
    plan tests => 2;
}

my $file1 = "one.tmp";
my $file2 = "two.tmp";
my $file3 = "three.tmp";
my $file4 = "four.tmp";

my $test = PMP::sge->new();

$test->name("test-pipeline");
$test->statusDir($pipeDir);


$test->addStage( 
    { name => "stage2",
      inputs => [$file3],
      outputs => [$file4],
      prereqs => ["test-stage"],
      args => ["touch", $file4]
    } );
$test->addStage( 
    { name => "test-stage",
      inputs => [],
      outputs => [$file3],
      args => ["touch", $file3] 
    } );
$test->addStage(
    { name => "test-memory",
      sge_opts => "-l vf=2G",
      args => ["sleep", 10]
    } );



#$test->printStage("test-stage");
#$test->printStage("stage2");

$test->subsetToStage("test-stage");

$test->updateStatus();

$test->printStages();
$test->sortStages();
$test->printStages();

my $continue = 1;
while ($continue) {
    $continue = $test->run();
}

ok(-f $test->getFinishedFile("test-stage"), 'first file exists' );
ok(! -f $test->getFinishedFile("stage2"), 'and second file does not' );

