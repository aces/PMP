#!/usr/bin/env perl -w

use strict;
use PMP::PMP;

my $file1 = "one.tmp";
my $file2 = "two.tmp";
my $file3 = "three.tmp";
my $file4 = "four.tmp";

my $test = PMP::PMP->new();
$test->name("test-pipeline");
$test->statusDir("/tmp/PMP");
system("mkdir -p /tmp/PMP") unless (-d "/tmp/PMP");

$test->addStage( 
    { name => "stage2",
      inputs => [$file3],
      outputs => [$file4],
      prereqs => ["test-stage"],
      args => ["touch", $file4]
    } );
$test->addStage( 
    { name => "test-stage",
      inputs => [$file1, $file2],
      outputs => [$file3],
      args => ["touch", $file3] 
    } );


#$test->printStage("test-stage");
#$test->printStage("stage2");

$test->updateStatus();

$test->printStages();
$test->sortStages();
$test->printStages();

my $continue = 1;
while ($continue) {
    $continue = $test->run();
}
