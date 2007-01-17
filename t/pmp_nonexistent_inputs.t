#!/usr/bin/env perl -w

# run a series of tests 

use strict;
use PMP::spawn;

use Test::More tests => 1;
use Test::Exception;

my $testDir = "/tmp/PMP";

my $test = PMP::spawn->new();
$test->name("test-pipeline");
$test->statusDir($testDir);
system("mkdir -p $testDir") unless (-d $testDir);

$test->addStage( 
    { name => "stage1",
      args => ["sleep", 1]
    });
$test->addStage(
    { name => "stage2",
      inputs => ["/this/file/does/not/exist"],
      args => ["sleep", 1],
      prereqs => ["stage1"]
    });

$test->computeDependenciesFromInputs();
$test->updateStatus();

dies_ok {
$test->run();
} 'updateStatus dies due to non-existent inputs';

system("rm -rf $testDir");
