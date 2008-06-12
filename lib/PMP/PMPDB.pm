###
# Package PMP::PMPDB - Poor Man's Pipeline with status and logs tracked in an
# sqlite database.
#
# To use this package you have to initialize the pipeline using one of
# the exec methods subclasses (PMP::sge, PMP::spawn, etc.) with the
# inheritance hierarchy overridden so that PMPDB is inserted in
# between the exec method subclass and PMP, i.e.:
# 
# @PMP::sge::ISA = ("PMP::PMPDB");
# my $test = PMP::sge->new();

package PMP::PMPDB;
use PMP::PMP;
use PMP::sge;
use PMP::spawn;
use DBI;
use MNI::MiscUtilities qw(shellquote timestamp);

@ISA = ("PMP::PMP");


# create the database tables.
sub createDatabase {
    my $self = shift;
    my $databaseName = shift;

    print "in DB-createDatabase\n" if $self->{DEBUG};

    # create the tables to be used
    $self->{DBH}->do("create table pipeline (name text)");
    $self->{DBH}->do("create table stage (name text, pipeline text, "
		     . "status text, status_date text, log text)");
    $self->{DBH}->do("create table PMP_info (PMP_version text, "
		     . "PMP_DB_schema integer)");
    $self->{DBH}->commit();
    # needs some error checking
}

# initialized a new stage with a 'not started' status in the database
sub createNewStageEntry {
    my $self = shift;
    my $stageName = shift;

    print "in DB-createNewStageEntry\n" if $self->{DEBUG};
    my $date = timestamp();

    # prepare the query for stage updating
    my $sth = $self->{DBH}->prepare_cached("insert into stage "
					   . "(name, pipeline, status, "
					   . "status_date)"
					   . "values (?, ?, ?, ?)");
    $sth->execute($stageName, $self->{NAME}, 'not started', $date);
    $sth->finish();
    # error checking?
}
    

# initialize the database, creating 'not started' statuses where necessary
sub initDatabase {
    my $self = shift;
    my $databaseName = shift;

    print "in DB-initDatabase\n" if $self->{DEBUG};

    $self->{databaseName} = $databaseName;

    my $date = timestamp();

    #$self->sortStages() unless $self->{isSorted};

    # configure and connect to the database
    my $dbargs = {AutoCommit => 0,
		  PrintError => 1};



    # if database exists, create any new stage with a 'not started' value
    if (-f $databaseName) {
	$self->{DBH} = DBI->connect("dbi:SQLite:dbname=$databaseName",
				    "","",$dbargs);

	print "it claims that the database exists: $databaseName\n" 
	    if $self->{DEBUG};
	foreach my $key ( keys %{ $self->{STAGES} } ) {
	    # check if key already exists in database
	    my $sth = $self->{DBH}->prepare_cached
		("select status from stage where name=? and pipeline = ?");
	    $sth->execute($key, $self->{NAME});
	    my $returnedKey = $sth->fetchrow_array();
	    $sth->finish();
	    # if key does not exist, create a new entry
	    $self->createNewStageEntry($key) unless $returnedKey;
	}
    }
    # if the database does not exist, create it
    else {
	$self->{DBH} = DBI->connect("dbi:SQLite:dbname=$databaseName",
				    "","",$dbargs);

	# create the table
	$self->createDatabase($databaseName);
	
	# set the database name
	$self->{DBH}->do("insert into pipeline (name) values ($self->{NAME})");

	# set each stage to a status of not started
	foreach my $key ( keys %{ $self->{STAGES} } ) {
	    print "DB - dealing with key $key\n" if $self->{DEBUG};
	    $self->createNewStageEntry($key);
	}
	$self->{DBH}->commit();
    }
}

# stageStatus - overloaded from PMP.
sub stageStatus {
    my $self = shift;
    my $stageName = shift;
    my $status = shift;

    my $returnVal = 0;


    unless (exists $self->{DBINIT}) {
	print "status does not exist\n" if $self->{DEBUG};
	unless (exists $self->{DBNAME}) {
	    die "ERROR: must set a database filename.\n";
	}
	$self->initDatabase($self->{DBNAME});
	$self->{DBINIT} = 1;

    }
    # check whether the status flag is set
    if ( $self->{STAGES}{$stageName}{$status} ) { 
	$returnVal = 1;
    }
    elsif (-f $self->getStageFile($stageName, $status)) {
	# status flag not set but status file exists - remove
	# status file and update the database
	$self->{STAGES}{$stageName}{$status} = 1;
	$self->updateDBStageStatus($stageName, $status);
	$self->slurpLog($stageName);
	unlink $self->getStageFile($stageName, $status);
	$returnVal = 1;
    }
    else {
	print "DB - inside else: $status\n" if $self->{DEBUG};
	# status flag not set - get from database
	print "prepare_cached for $stageName $self->{NAME}\n" if $self->{DEBUG};
	my $sth = $self->{DBH}->prepare_cached("SELECT status from stage "
					       . "where name = ? " 
					       . "AND pipeline = ?");
	$sth->execute($stageName, $self->{NAME});
	my $returnedStatus = $sth->fetchrow_array();
	if ($returnedStatus eq $status) {
	    # status matched - update in internal records
	    $returnVal = 1;
	    $self->{STAGES}{$stageName}{$status} = 1;
	}
	$sth->finish();
    }
    $self->{DBH}->commit();

    print "in DB-stageStatus: $stageName $status $returnVal\n" 
	if $self->{DEBUG};

    return $returnVal;
}

# resetStage - overridden from PMP. Calls the superclass version of 
# resetStage and also changes the entry in the database.
sub resetStage {
    my $self = shift;
    my $stageName = shift;

    # call superclass
    $self->SUPER::resetStage($stageName);
    $self->updateDBStageStatus($stageName, "not started");
}

sub setDBName {
    my $self = shift;
    my $fileName = shift;

    $self->{DBNAME} = $fileName;
}

sub updateStatus {
    my $self = shift;

    print "updateStatus for $self->{NAME}\n" if $self->{DEBUG};    
    # if DB was never initialized, do it now
    unless (exists $self->{DBINIT}) {
	print "status does not exist\n" if $self->{DEBUG};
	unless (exists $self->{DBNAME}) {
	    die "ERROR: must set a database filename.\n";
	}
	$self->initDatabase($self->{DBNAME});
	$self->{DBINIT} = 1;

    }

    # call superclass
    return $self->SUPER::updateStatus();
    
}

# declareStageRunning - call superclass, update DB status
sub declareStageRunning {
    my $self = shift;
    my $stageName = shift;

    $self->SUPER::declareStageRunning($stageName);
    $self->updateDBStageStatus($stageName, "running");

    print "inside declareStageRunning: $stageName\n" if $self->{DEBUG};
}
    

# updateDBStageStatus - updates the status of a stage in the database
sub updateDBStageStatus {
    my $self = shift;
    my $stageName = shift;
    my $newStatus = shift;

    my $date = timestamp();

    my $sth = $self->{DBH}->prepare_cached
	("update stage set status = ?, status_date = ? "
	 . "where name = ? and pipeline = ?");
    $sth->execute($newStatus, $date, $stageName, $self->{NAME});
    $sth->finish();
    $self->{DBH}->commit();
}

# slurpLog - read the log file into the database, remove log file when done
sub slurpLog {
    my $self = shift;
    my $stageName = shift;

    if (-f $self->getLogFile($stageName)) {
	# use the separator trick to read whole file at once
	my $oldSep = $/;
	undef $/;
	open(LOGFILE, $self->getLogFile($stageName));
	my $logText = <LOGFILE>;
	my $sth = $self->{DBH}->prepare_cached
	    ("update stage set log = ? where name = ? and pipeline = ?");
	$sth->execute($logText, $stageName, $self->{NAME});
	$sth->finish();
	$self->{DBH}->commit();
	close(LOGFILE);
	unlink $self->getLogFile($stageName);
	$/ = $oldSep;
    }
}

    
1;
