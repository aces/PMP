#!/usr/bin/env perl -w

# run a series of tests 

use strict;
use PMP::spawn;

use Test::More tests => 1;
use Test::Exception;

my $testDir = "/tmp/PMP";

my $file1 = "$testDir/one.tmp";
my $file2 = "$testDir/two.tmp";
my $file3 = "$testDir/three.tmp";
my $file4 = "$testDir/four.tmp";

my $test = PMP::spawn->new();
$test->name("test-pipeline");
$test->statusDir($testDir);
system("mkdir -p $testDir") unless (-d $testDir);

$test->addStage( 
    { name => "stage1",
      inputs => [],
      outputs => [$file1],
      args => ["touch", $file1] 
    } );
$test->addStage( 
    { name => "stage2",
      inputs => [$file1],
      outputs => [$file2],
      prereqs => ["test-stage3"], #doesn't actually exist
      args => ["touch", $file2]
    } );
$test->addStage(
    { name => "stage3", 
      inputs => [$file1],
      outputs => [$file3],
      prereqs => ["stage1"],
      args => ["touch", $file3]
    } );
$test->addStage(
    { name => "stage4",
      inputs => [$file3],
      outputs => [$file4],
      prereqs => [],
      args => ["touch", $file4]
    });

dies_ok {
$test->updateStatus();
} 'updateStatus dies due to incorrect prereq specification';

# $test->cleanLockFile();
# $test->initLockFile();

# my $continue = 1;
# while ($continue) {
#     print "in run\n";
#     $continue = $test->run();
# }

# # test that file with no prereqs exists
# ok(-f $test->getFinishedFile("stage4"), "file exits" );
# # make sure file with incorrect prereq is created anyway
# ok(! -f $test->getFinishedFile("stage2"), "file exists" );

# # check to make sure filedates are correct
# my @stat2 = stat $test->getFinishedFile("stage2");
# my @stat3 = stat $test->getFinishedFile("stage3");
# ok($stat3[9] > $stat2[9], "file times correct");
# print "time1: $stat2[9] time2: $stat3[9]\n";

# # make sure stage with no prereqs is run
# ok(-f $test->getFinishedFile("stage4"), "file exists" );


system("rm -rf $testDir");
