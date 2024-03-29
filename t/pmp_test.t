#!/usr/bin/env perl

use strict;
use warnings "all";
use PMP::spawn;
use Test::Simple tests => 30;

my $testDir = "/tmp/PMP";

my $file1 = "$testDir/one.tmp";
my $file2 = "$testDir/two.tmp";
my $file3 = "$testDir/three.tmp";
my $file4 = "$testDir/four.tmp";
my $file5 = "$testDir/five.tmp";
my $file6 = "$testDir/six.tmp";

my $dotGraph = "dotgraph.dot";

# initialize the pipeline
my $test = PMP::spawn->new();
$test->name("test-pipeline");

# where to put the status files
$test->statusDir($testDir);
system("mkdir -p $testDir") unless (-d $testDir);

#$test->resetAll();

# the actual stages - they use either the sleep or the touch command
$test->addStage( 
    { name => "stage1",
      inputs => [],
      outputs => [$file1],
      args => ["touch", $file1]
      });
$test->addStage( 
    { name => "stage2",
      inputs => [$file1],
      outputs => [$file2],
      args => ["touch", $file2],
      prereqs => ["stage1"]
      });
$test->addStage( 
    { name => "stage3",
      inputs => [$file1],
      outputs => [$file3],
      args => ["touch", $file3],
      prereqs => ["stage1"]
      });
$test->addStage( 
    { name => "stage4",
      inputs => [$file2, $file3],
      outputs => [$file4],
      args => ["touch", $file4],
      prereqs => ["stage2", "stage3"]
      });
$test->addStage( 
    { name => "stage5",
      inputs => [$file4],
      outputs => [$file5],
      args => ["touch", $file5],
      prereqs => ["stage4"]
      });
$test->addStage( 
    { name => "stage6",
      inputs => [$file5],
      outputs => [$file6],
      prereqs => ["stage5"],
      args => ["touch", $file6]
      });

$test->updateStatus();
$test->createDotGraph($dotGraph);

# run this pipeline
my $continue = 1;
while ($continue) {
    $continue = $test->run();
}

# first set of tests
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );  #1
ok(-f $test->getFinishedFile("stage2"), 'second file exists' ); #2
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );  #3
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' ); #4
ok(-f $test->getFinishedFile("stage5"), 'fifth file exists' );  #5
ok(-f $test->getFinishedFile("stage6"), 'sixth file exists' );  #6

# now reset from a stage
$test->resetFromStage("stage4");
$test->updateStatus();

# make sure correct files exist
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );  #7
ok(-f $test->getFinishedFile("stage2"), 'second file exists' ); #8
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );  #9
ok(! -f $test->getFinishedFile("stage4"), 'fourth file gone' ); #10
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );  #11
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );  #12

# run this pipeline
$continue = 1;
while ($continue) {
    $continue = $test->run();
}

# all files should exist again
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );  #13
ok(-f $test->getFinishedFile("stage2"), 'second file exists' ); #14
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );  #15
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' ); #16
ok(-f $test->getFinishedFile("stage5"), 'fifth file exists' );  #17
ok(-f $test->getFinishedFile("stage6"), 'sixth file exists' );  #18

# now reset all and make sure finished files are gone.
$test->resetAll();
$test->updateStatus();

ok(! -f $test->getFinishedFile("stage1"), 'first file gone' );  #19
ok(! -f $test->getFinishedFile("stage2"), 'second file gone' ); #20
ok(! -f $test->getFinishedFile("stage3"), 'third file gone' );  #21
ok(! -f $test->getFinishedFile("stage4"), 'fourth file gone' ); #22
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );  #23
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );  #24

# now subset the pipeline and ensure that only the correct files exist
$test->subsetToStage("stage4");
# run this pipeline

$continue = 1;
while ($continue) {
    $continue = $test->run();
}

ok(-f $test->getFinishedFile("stage1"), 'first file exists' );  #25
ok(-f $test->getFinishedFile("stage2"), 'second file exists' ); #26
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );  #27
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' ); #28
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );  #29
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );  #30

system("rm -rf $testDir");
