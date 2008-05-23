package AsyncCaller;

use strict;
use base ( "Exporter" );

use Event::Lib;

our @EXPORT_OK = qw(
    schedule
    schedule_regular
    schedule_regular_delayed
);

sub schedule
{
    my $delay = shift;
    my $func;
    unless ( ref( $delay ) ) {
        $func = shift;
    } else {
        $func = $delay;
        $delay = 0;
    }
    $delay += 0.00001;
    my @params = @_;
    my $event = timer_new( \&_execute_scheduled, $func, @params );
    $event->add( $delay );
    return $event;

}

sub schedule_regular
{
    my $tm = shift;
    my $func = shift;
    my @params = @_;
    my $event = timer_new( \&_execute_scheduled_regular, $tm, $func, @params );
    $event->add( $tm );
}

sub schedule_regular_delayed
{
    my $delay = shift;
    my $tm = shift;
    my $func = shift;
    my @params = @_;
    my $event = timer_new( \&_execute_scheduled_regular, $tm, $func, @params );
    $event->add( $delay );
}

sub _execute_scheduled_regular
{
    my ( $event, $type, $tm, $func, @params ) = @_;
    $event->add( $tm );
    &$func( @params );
}

sub _execute_scheduled
{
    my ( $event, $type, $func, @params ) = @_;
    &$func( @params );
}

1;


