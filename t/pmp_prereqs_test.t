#!/usr/bin/env perl -w

# run a series of tests 

use strict;
use PMP::PMP;

use Test::More tests => 4;

my $file1 = "one.tmp";
my $file2 = "two.tmp";
my $file3 = "three.tmp";
my $file4 = "four.tmp";

my $test = PMP::PMP->new();
$test->name("test-pipeline");
$test->statusDir("/tmp/PMP");
system("mkdir -p /tmp/PMP") unless (-d "/tmp/PMP");

$test->addStage( 
    { name => "stage1",
      inputs => [$file1],
      outputs => [$file2],
      args => ["touch", $file2] 
    } );
$test->addStage( 
    { name => "stage2",
      inputs => [$file2],
      outputs => [$file3],
      prereqs => ["test-stage3"], #doesn't actually exist
      args => ["sleep", 2]
    } );
$test->addStage(
    { name => "stage3", 
      inputs => [$file3],
      outputs => [$file4],
      prereqs => ["stage2"],
      args => ["sleep", 1]
    } );
$test->addStage(
    { name => "stage4",
      inputs => [$file3],
      outputs => [$file4],
      prereqs => [],
      args => ["sleep", 1]
    });

my $continue = 1;
while ($continue) {
    print "in run\n";
    $continue = $test->run();
}

# test that file with no prereqs exists
ok(-f $test->getFinishedFile("stage4"), "file exits" );
# make sure file with incorrect prereq is created anyway
ok(-f $test->getFinishedFile("stage2"), "file exists" );

# check to make sure filedates are correct
my @stat2 = stat $test->getFinishedFile("stage2");
my @stat3 = stat $test->getFinishedFile("stage3");
ok($stat3[9] > $stat2[9], "file times correct");
print "time1: $stat2[9] time2: $stat3[9]\n";

# make sure stage with no prereqs is run
ok(-f $test->getFinishedFile("stage4"), "file exists" );

system("rm -rf /tmp/PMP");
