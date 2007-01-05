#!/usr/bin/env perl -w

use strict;
use PMP::PMP;
use PMP::sge;

use Test::More;
# test if SGE's qsub is installed at all
my $pipeDir = "/projects/mice/jlerch/test-PMP-pipeline";
system("mkdir $pipeDir") unless (-d $pipeDir);

my $sge_message = `qsub -help`;
if ( $sge_message =~ /SGE/ and -w $pipeDir) {
    plan tests => 2;
}
else {
    plan skip_all => "SGE not installed or $pipeDir not writeable: skipping test";
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
system("rm -rf $pipeDir");
