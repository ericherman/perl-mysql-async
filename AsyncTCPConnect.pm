package AsyncTCPConnect;
use strict;
use base 'Exporter';

our @EXPORT_OK = qw/
    connect_async    
/; 


use Carp qw/carp/;
use Event::Lib;
use Fcntl qw/F_GETFL F_SETFL O_NONBLOCK/;
use POSIX qw/:errno_h/;
use Socket;

use constant DEFAULT_TIMEOUT => 5;

sub _carp
{
    local $Carp::CarpLevel = 1;
    carp(@_);
    return undef;
}

sub connect_async
{
    my $params = {@_};
    my $conn_data = {
        host             => $params->{host} || $params->{PeerAddr}, 
        port             => $params->{port} || $params->{PeerPort},
        timeout          => $params->{timeout} || $params->{Timeout} || DEFAULT_TIMEOUT,
        on_connect       => $params->{on_connect},
        on_connect_param => $params->{on_connect_param},
    };
    return _carp("connect_async: host, port and on_connect callback must be specified\n")
        unless( $conn_data->{host} && $conn_data->{port} && $conn_data->{on_connect} );
    
    my $proto = getprotobyname('tcp');
    my $sock;
    socket($sock, PF_INET, SOCK_STREAM, $proto) or return _carp("connect_async: can't create socket\n");
    
    my $flags = fcntl($sock, F_GETFL, 0) or return _carp("connect_async: can't get flags for the socket: $!\n");
    $flags = fcntl($sock, F_SETFL, $flags | O_NONBLOCK) or return _carp( "connect_async: can't set flags for the socket: $!\n");   
    
    my $iaddr = inet_aton($conn_data->{host});
    return _carp( "connect_async: bad host $!" ) unless $iaddr;
    my $s_addr = sockaddr_in( int $conn_data->{port}, $iaddr );
    if( connect($sock, $s_addr) ){
        my $conn_event = timer_new( \&_on_connected, $conn_data );
        $conn_event->add(0.00000001);
    } else {
        if( $! == EINPROGRESS ) {
            my $conn_event = event_new( $sock, EV_WRITE, \&_on_connected, $conn_data );
            $conn_event->add();
            
            my $timeout_event = timer_new( \&_on_timeout, $conn_event, $conn_data );
            $timeout_event->add( $conn_data->{timeout} );
        } else {
            return _carp( "connect_async: connect failed: $!\n" );
        }
    }
    return $sock;
}


sub _on_timeout
{
    my ( $event, $type, $conn_event, $conn_data ) = @_;
    return if $conn_data->{connected};
    $conn_event->remove();
    $conn_data->{on_connect}->( undef, $conn_data->{on_connect_param} );
}

sub _on_connected
{
    my ( $event, $type, $conn_data ) = @_;
    $conn_data->{connected} = 1;
    $conn_data->{on_connect}->( $event->fh(), $conn_data->{on_connect_param} );
}

