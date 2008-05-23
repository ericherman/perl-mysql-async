#!/usr/bin/perl -w
use strict;

use Event::Lib;
use Data::Dumper;

use MysqlAsync;
use AsyncCaller qw/schedule/;

$Data::Dumper::Terse = 1;

$|=1;


my $dbh = MysqlAsync->new(
     database => {
        host => "",
        port => 3306,
        database => "",
        passwd => "",
        user => "",
    },
   connect_timeout => 5,
    max_requests    => 5,
    db_timeout      => 2,
    logfile         => "/person/var/local/log/__mysql_async_test",
);

$dbh->get_scalar("SELECT NOW()", \&result );

schedule( 40, sub{  
    $dbh->get_array("SELECT 123", \&result );
    $dbh->get_array("SELECT 1,2,3", \&result )
 } );

schedule( 80, sub{  
    $dbh->get_array("SELECT 444", \&result );
    $dbh->get_array("SELECT 5,6,7", \&result )
 } );


event_mainloop();
    
sub result    
{
    my ( $result ) = @_;
    if( defined $result ) {
        print "result: ".Dumper( $result );
    } else {
        print "error: ".Dumper( $dbh->error() );
    }
}
    
