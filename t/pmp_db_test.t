#!/bin/env perl

use strict;
use warnings;
use PMP::PMP;
use PMP::spawn;
use PMP::PMPDB;
use DBI;
use Test::Simple tests => 1;

my $testDir = "/tmp/PMP-test";

system("mkdir -p $testDir") unless -d $testDir;

# initialize the pipeline
@PMP::spawn::ISA = ("PMP::PMPDB");
my $test = PMP::spawn->new();
$test->debug(0);
$test->name("test-pipeline");

my $databaseName = "$testDir/status.db";
$test->setDBName($databaseName);


# where to put the status files
$test->statusDir($testDir);
system("mkdir -p $testDir") unless (-d $testDir);

# the actual stages
my $input = "$testDir/input.mnc";
$test->addStage(
    { name => "createinput",
      args => ["rawtominc", "-input", "/dev/urandom", "out:$input", 
	       10, 10, 10] });
my $first = "$testDir/first.mnc";
$test->addStage(
    { name => "firstaddition",
      args => ["mincmath", "-clobber", "-add", "-const", 1, "in:$input",
	       "out:$first"] });
my $second = "$testDir/second.mnc";
$test->addStage(
    { name => "firstsubtraction",
      args => ["mincmath", "-clobber", "-sub", "-const", 3, "in:$input",
	       "out:$second"] });
my $final = "$testDir/final.mnc";
$test->addStage(
    { name => "avg",
      args => ["mincaverage", "-clobber", "in:$first", "in:$second",
	       "out:$final"] });

$test->computeDependenciesFromInputs();
#$test->initDatabase("$testDir/status.db");
$test->updateStatus();
$test->createDotGraph("test.dot");

$test->cleanLockFile();
$test->initLockFile();

# run this pipeline
my $continue = 1;
while ($continue) {
    $continue = $test->run();
}

my $dbargs = {AutoCommit => 1,
	      PrintError => 1};

my $dbh = DBI->connect("dbi:SQLite:dbname=$databaseName",
		       "","",$dbargs);
my $sth = $dbh->prepare("select status from stage where name = ?");
$sth->execute("avg");
my $returnedStatus = $sth->fetchrow_array();
$sth->finish();
ok($returnedStatus eq "finished");
$dbh->disconnect();
