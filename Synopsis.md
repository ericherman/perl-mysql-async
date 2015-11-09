
```
#!/usr/bin/perl -w
use strict;

use Event::Lib;
use Data::Dumper;

use MysqlAsync;
use AsyncCaller qw/schedule/;

my $dbh = MysqlAsync->new(
     database => {
        host => "myhost",
        port => 3306,
        database => "mydb",
        passwd => "pwd",
        user => "dbuser",
    },
    connect_timeout => 5,
    max_requests    => 5,
    db_timeout      => 5,
    default_request_timeout => 10,   
    logfile         => "/var/log/mylog",
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
```