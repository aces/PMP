#!/usr/bin/env perl -w

use strict;
use PMP::PMP;
use Test::Simple tests => 30;

my $file1 = "one.tmp";
my $file2 = "two.tmp";
my $file3 = "three.tmp";
my $file4 = "four.tmp";

# initialize the pipeline
my $test = PMP::PMP->new();
$test->name("test-pipeline");

# where to put the status files
$test->statusDir("/tmp/PMP");
system("mkdir -p /tmp/PMP") unless (-d "/tmp/PMP");

# the actual stages - they use either the sleep or the touch command
$test->addStage( 
    { name => "stage1",
      inputs => [$file3],
      outputs => [$file4],
      args => ["sleep", 2]
      });
$test->addStage( 
    { name => "stage2",
      inputs => [$file3],
      outputs => [$file4],
      args => ["sleep", 2],
      prereqs => ["stage1"]
      });
$test->addStage( 
    { name => "stage3",
      inputs => [$file3],
      outputs => [$file4],
      args => ["sleep", 2],
      prereqs => ["stage1"]
      });
$test->addStage( 
    { name => "stage4",
      inputs => [$file3],
      outputs => [$file4],
      args => ["sleep", 2],
      prereqs => ["stage2", "stage3"]
      });
$test->addStage( 
    { name => "stage5",
      inputs => [$file3],
      outputs => [$file4],
      args => ["sleep", 2],
      prereqs => ["stage4"]
      });
$test->addStage( 
    { name => "stage6",
      inputs => [$file3],
      outputs => [$file4],
      prereqs => ["stage5"],
      args => ["touch", $file4]
      });

$test->updateStatus();

# run this pipeline
my $continue = 1;
while ($continue) {
    $continue = $test->run();
}

# first set of tests
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );
ok(-f $test->getFinishedFile("stage2"), 'second file exists' );
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' );
ok(-f $test->getFinishedFile("stage5"), 'fifth file exists' );
ok(-f $test->getFinishedFile("stage6"), 'sixth file exists' );

# now reset from a stage
$test->resetFromStage("stage4");
$test->updateStatus();

# make sure correct files exist
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );
ok(-f $test->getFinishedFile("stage2"), 'second file exists' );
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );
ok(! -f $test->getFinishedFile("stage4"), 'fourth file gone' );
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );

# run this pipeline
$continue = 1;
while ($continue) {
    $continue = $test->run();
}

# all files should exist again
ok(-f $test->getFinishedFile("stage1"), 'first file exists' );
ok(-f $test->getFinishedFile("stage2"), 'second file exists' );
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' );
ok(-f $test->getFinishedFile("stage5"), 'fifth file exists' );
ok(-f $test->getFinishedFile("stage6"), 'sixth file exists' );

# now reset all and make sure finished files are gone.
$test->resetAll();
$test->updateStatus();

ok(! -f $test->getFinishedFile("stage1"), 'first file gone' );
ok(! -f $test->getFinishedFile("stage2"), 'second file gone' );
ok(! -f $test->getFinishedFile("stage3"), 'third file gone' );
ok(! -f $test->getFinishedFile("stage4"), 'fourth file gone' );
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );

# now subset the pipeline and ensure that only the correct files exist
$test->subsetToStage("stage4");
# run this pipeline
$continue = 1;
while ($continue) {
    $continue = $test->run();
}

ok(-f $test->getFinishedFile("stage1"), 'first file exists' );
ok(-f $test->getFinishedFile("stage2"), 'second file exists' );
ok(-f $test->getFinishedFile("stage3"), 'third file exists' );
ok(-f $test->getFinishedFile("stage4"), 'fourth file exists' );
ok(! -f $test->getFinishedFile("stage5"), 'fifth file gone' );
ok(! -f $test->getFinishedFile("stage6"), 'sixth file gone' );

