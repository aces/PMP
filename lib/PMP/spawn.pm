###
# Package PMP::spawn - Poor Man's Pipeline using the MNI::Spawn batch system
#

package PMP::spawn;
use PMP::PMP;
use MNI::Spawn;
use MNI::Startup;
use MNI::MiscUtilities qw(shellquote);

@ISA = ("PMP::PMP");

# exec a stage through the MNI::Spawn interface
sub execStage {
    my $self = shift;
    my $stageName = shift;

    # run the stage in question
    $self->declareStageRunning($stageName);
    my $runningFile = $self->getRunningFile($stageName);

    my $logFile = $self->getLogFile($stageName);
    my $finishedFile = $self->getFinishedFile($stageName);
    my $failedFile = $self->getFailedFile($stageName);

    my $status = Spawn($self->{STAGES}{$stageName}{'args'},
                       stdout => $logFile, err_action => 'ignore' );

    if ($status != 0) {
        system( "touch $failedFile" );
    } else {
        system( "touch $finishedFile" );
    }
    unlink $runningFile;

}

