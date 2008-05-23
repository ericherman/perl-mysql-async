{
package MysqlAsync;
use strict;

use IO::Socket::INET;
use Event::Lib;
use Data::Dumper;
use Time::HiRes qw(time usleep);

use AsyncTCPConnect qw/connect_async/;

use MysqlPacket qw(:debug);           # dumping packet contents etc.
use MysqlPacket qw(:test :decode);    # decoding subs
use MysqlPacket qw(:encode);          # encoding subs
use MysqlPacket qw(:crypt);           # crypt subs

use MysqlPacket  qw(:COM :CLIENT :SERVER);     # constants

use AsyncCaller qw/schedule schedule_regular/;

use constant { 
    NEED_REDO => 1,
    NEED_RECONNECT => 2,
    NEED_DATA => 3,
};

use constant { 
    ERRNO_OVERFLOW          => -1001,
    ERRNO_TIMEOUT           => -1002,
    ERRNO_NO_MORE_TRIES     => -1003,
    ERRNO_DB_TIMIOUT        => -1004,
};

#сколько ждать если не удалось реконнектнуться перед следующим реконнектом
use constant RECONNECT_INTERVAL => 10;

#сколько раз запрос имеет право быть переотосланным из-за непредвиденных ошибок 
use constant MAX_REQUEST_TRIES => 3;

use constant {
    CHECK_DB_TIMEOUT_INTERVAL       => 3,
    CHECK_REQUEST_TIMEOUT_INTERVAL  => 1,
};

use constant RESPONSE_UNPACK_FUNC => {
    get_array       => '_get_array_unpack',
    get_plain_array => '_get_plain_array_unpack',
    get_hash        => '_get_hash_unpack',
    get_plain_hash  => '_get_plain_hash_unpack',
    get_row         => '_get_row_unpack',
    get_plain_row   => '_get_plain_row_unpack',
    get_scalar      => '_get_scalar_unpack',
    
    insert          => '_insert_unpack',
    update          => '_update_unpack',
    delete          => '_update_unpack',
    execute         => '_update_unpack',
};


sub new
{
    my $caller = shift;
    my $params = {@_};
    my $database = $params->{database};
    $database->{port} ||= 3306;
    unless( $database ) {
        print STDERR "MysqlAsync: no database specified\n";
        return undef;
    }   
    
    $database->{database} ||=  ( $database->{db_name} || $database->{db_names}->{main} );
    
    
    #print STDERR "MysqlAsync:new: ".Dumper( $params );
    my $self = {
        database                    => $database,
        
        #таймаут на коннет к бд
        connect_timeout             => $params->{connect_timeout} || 2,
        
        #если запрос провисит дольше этого времени, то вызовется его коллбэк с ошибкой
        default_request_timeout     => $params->{default_request_timeout},
        logfile                     => $params->{logfile},
        max_log_lines                => $params->{max_log_lines},
        db_timeout                  => $params->{db_timeout},
        max_requests                => $params->{max_requests},
        reconnect_interval          => $params->{reconnect_interval} || RECONNECT_INTERVAL,
        on_failed_reconnect         => $params->{on_failed_reconnect},
        on_failed_reconnect_param   => $params->{on_failed_reconnect_param},
        last_id                     => 1,
        requests                    => [],
        pending_requests            => {},    
    };
    bless( $self, ref($caller) || $caller );
    $self->connect() unless $params->{do_not_connect};
    AsyncCaller::schedule_regular( CHECK_DB_TIMEOUT_INTERVAL, \&_check_db_timeout, $self )
        if $self->{db_timeout}; 
    
    AsyncCaller::schedule_regular( CHECK_REQUEST_TIMEOUT_INTERVAL, \&_check_timeout_requests, $self );
       
        
    if( $self->{logfile} ) {
        $self->rotate_log();
    }        
        
        
    return $self;
}    
    
  
sub rotate_log
{
    my ( $self ) = @_;
    my $fh;
    if( $self->{logfh} ) {
        close delete $self->{logfh};
    }
    $self->{log_lines} = 0;
    my $full_logfile_name = $self->{logfile}.".".CORE::time();
    my $postfix = 0;
    while( -e $full_logfile_name ) {
        $postfix++;
        $full_logfile_name.=('.'.$postfix);
    }
    my $res = open $fh, ">$full_logfile_name";
    if( $res ) {
        $self->{logfh} = $fh;
        $fh->autoflush(1);
    } else {
        print STDERR "MysqlAsync: can't open logfile $full_logfile_name\n";
    }
}

sub log
{
    my ( $self, $format, @args ) = @_;
    #printf STDERR "MysqlAsync::log: $format", @args;
    my $fh = $self->{logfh};
    return unless $fh;
    #printf STDERR "MysqlAsync::log1: $format", @args;
    $self->{log_lines} ||= 0;
    printf $fh $format, @args;    
    $self->{log_lines}++;
    $self->rotate_log()
        if $self->{max_log_lines} && $self->{log_lines} > $self->{max_log_lines};
}
    
sub dsn
{
    my ( $self ) = @_;
    my $database = $self->{database};
    return 'mysql:database='.$database->{database}.';host='.$database->{host}.';port='.$database->{port}.';user='.$database->{user};
}    
    
sub connect
{
    my ( $self, $callback ) = @_;
    $self->disconnect() if $self->{connect};
    my $database = $self->{database};
    connect_async( 
        host => $database->{host}, 
        port => $database->{port},
        on_connect => $callback || \&_connected,
        on_connect_param => $self,
    );
    $self->query( "use $database->{database}" );  
}
    
sub _connected
{
    my ( $sock, $self ) = @_;    
    my $database = $self->{database};
    unless( $sock ) {
        print STDERR "MysqlAsync: can't connect to $database->{host}:$database->{port}\n";
        return undef;
    }
    #$sock->autoflush(1);
    #$sock->blocking(0);
    
    $self->{connect}->{sock} = $sock;
    $self->{connect}->{data} = '';
    
    my $read_event = event_new( $sock, EV_READ | EV_PERSIST , \&_read_data, $self );
    #$read_event->add();
    $self->{connect}->{read_event} = $read_event;
    
    my $write_event = event_new( $sock, EV_WRITE, \&_write_data, $self );
    $self->{connect}->{write_event} = $write_event;
    
    $self->can_read(1);
    
    delete $self->{reconnect_fails};
    
    
    print STDERR "MysqlAsync: connected to $database->{host}:$database->{port}\n";
}
    
sub disconnect
{
    my ( $self ) = @_;
    my $connect = delete $self->{connect};
    
    my $read_event = delete $connect->{read_event};
    $read_event->remove() if $read_event;
    
    my $write_event = delete $connect->{write_event};
    $write_event->remove() if $write_event;
    
    my $sock = delete $connect->{sock};
    close $sock if $sock;
    
}    

sub error_reconnect
{
    my ( $self, $msg ) = @_;
    print STDERR  $msg;
    #die $msg;
    $self->{saved_requests} = [ grep { $_->{query} ne 'raw_data' } @{$self->{requests}} ];
    $self->{requests} = [];
    $self->disconnect();
    $self->connect( \&_error_reconnected );
    return NEED_RECONNECT;
}    
sub _error_reconnected    
{
    my ( $sock, $self ) = @_;
    _connected( $sock, $self );
    my $connect = $self->{connect};
    if( $connect ) {
        push @{$self->{requests}}, @{$self->{saved_requests}};
        $self->{saved_requests} = [];
        #print STDERR "MysqlAsync::error_reconnect: reconnected: \$connect=".Dumper( $connect );
    } else {
        $self->{reconnect_fails}++;
        if( !$self->{on_failed_reconnect} || $self->{on_failed_reconnect}->( $self, $self->{on_failed_reconnect_param} ) ) {
            AsyncCaller::schedule( $self->{reconnect_interval}, \&error_reconnect, $self, "MysqlAsync: reconnecting\n" );
        }
    }
}
    

sub reconnect_fails
{
    my ( $self ) = @_;
    return $self->{reconnect_fails} || 0;
}


sub error
{
    my ( $self ) = @_;
    return $self->{connect} && $self->{connect}->{error};
}
    
sub query {
    my ( $self, $query, $callback, $callback_param, $timeout  ) = @_;
    my $connect = $self->{connect};
    #unless( $connect ) {
    #    print STDERR "MysqlAsync::query: not connected!\n";
    #    return;
    #}
    if( $self->{max_requests} && @{$self->{requests}} >= $self->{max_requests} ) {
        print STDERR "MysqlAsync: request queue overflow: $self->{max_requests} requests aready pending into ".$self->dsn()."\n";
        AsyncCaller::schedule( 
            \&_handle_failed_request, 
            $self, 
            $callback, 
            $callback_param,
            {
                errno => ERRNO_OVERFLOW,
                message => "MysqlAsync: request queue overflow\n",
            } 
        ) if $callback;
        return;
    } 
    $timeout ||= $self->{default_request_timeout};
    
    my $packet_body = mysql_encode_com_query( $query );
    my $packet_head = mysql_encode_header( $packet_body );
    my $data = $packet_head.$packet_body;
    my $request = {
        query           =>  $query, 
        data            =>  $data, 
        offset          =>  0,
        length          =>  length( $data ),
        callback        =>  $callback, 
        callback_param  =>  $callback_param,
        ctime           =>  time(),
        id              =>  $self->{last_id}++,
    };
    push  @{$self->{requests}}, $request;
    if( $timeout ) {
        #print STDERR "MysqlAsync: _check_timeout_request scheduled in $timeout seconds\n";
        #$request->{event} = AsyncCaller::schedule( $timeout, \&_check_timeout_request, $self, $request->{id} );
        $self->{pending_requests}->{$request->{id}} = time() + $timeout;
    }
    delete $connect->{last_insert_id};
    delete $connect->{affected_rows};
    $self->_add_write_event() if $self->{connect} && $self->can_write();
    return 1;
}


sub _check_timeout_requests
{
    #print STDERR "MysqlAsync: _check_timeout_requests\n";
    my ( $self ) = @_;
    
    my $requests = $self->{requests};
    
    my $time = time();
    my $pending_requests = $self->{pending_requests};
    my @ids = grep { $pending_requests->{$_} < $time } keys %$pending_requests;
    delete @$pending_requests{@ids};
    
    foreach my $id ( @ids ) {
    
        if ( @$requests && $requests->[0]->{id} && $requests->[0]->{id} <= $id ) {
            my ($index) = grep { $requests->[$_]->{id} == $id } 0..$#$requests;
            return unless defined $index;
            my $timeout_request = $index == 0 
                ? $requests->[0]
                : splice( @$requests, $index, 1 );
            my ( $callback, $callback_param ) = @$timeout_request{qw/callback callback_param/};
            $self->_handle_failed_request( 
                $callback, 
                $callback_param,
                {
                    errno => ERRNO_TIMEOUT,
                    message => "MysqlAsync: request timeout\n",
                } 
            ) if $callback;
            delete $timeout_request->{callback};
        }
    
    }
}


sub _handle_failed_request
{
    my ( $self, $callback, $callback_param, $error ) = @_;
    return unless $callback;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::_handle_owerflowed_request: not connected!\n";
        return;
    }
    $connect->{error} = $error;
    $callback->( undef, $callback_param );
}
    
sub quote
{
    my ($self, $statement) = @_;
    return 'NULL' unless defined $statement;

    for ($statement) {
        s/\\/\\\\/g;
        s/\0/\\0/g;
        s/\n/\\n/g;
        s/\r/\\r/g;
        s/'/\\'/g;
        s/"/\\"/g;
        s/\x1a/\\Z/g;
    }
    return "'$statement'";
}
    
    
sub _send_client_auth 
{
    #print STDERR "MysqlAsync: send_client_auth\n";
    my ( $self ) = @_;
    my $greeting = $self->{connect}->{greeting};
    #mysql_debug_packet $greeting;
    my $database = $self->{database};    
    my $flags = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION;
    #my $flags = 0;
    #$flags |= CLIENT_CONNECT_WITH_DB if $i_want_to;
    #print STDERR "MysqlAsync: passwd:[$conn_settings->{passwd}], crypt_seed:[$greeting->{crypt_seed}]\n";
    my $pw_crypt = mysql_crypt( $database->{passwd}, $greeting->{crypt_seed} );
    #print STDERR "MysqlAsync: mysql_encode_client_auth\n";
    my $packet_body = mysql_encode_client_auth (
        $flags,                                 # $client_flags
        0x01000000,                             # $max_packet_size
        $greeting->{server_lang},               # $charset_no
        $database->{user},                 # $username
        $pw_crypt,                              # $pw_crypt
        $database->{database},             # $database
    );
    my $packet_head = mysql_encode_header( $packet_body, 1 );
    $self->_send_data( $packet_head.$packet_body );
}

sub _send_data 
{
        #print STDERR "MysqlAsync::send_data\n";
    my ( $self, $data ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::can_read: not connected!\n";
        return;
    }
    if ( !$connect->{current_request} ) {
        unshift @{$self->{requests}}, {
            query           =>  'raw_data', 
            data            =>  $data, 
            offset          =>  0,
            length          =>  length( $data ),
            callback        =>  undef, 
            callback_param  =>  undef,
        };
    } else {
        my $req = shift @{$self->{requests}};
        unshift @{$self->{requests}}, {
            query           =>  'raw_data', 
            data            =>  $data, 
            offset          =>  0,
            length          =>  length( $data ),
            callback        =>  undef, 
            callback_param  =>  undef,
        };
        unshift @{$self->{requests}}, $req;
    }
    $self->_add_write_event() if $self->can_write();
}


sub _write_data
{
    #print STDERR "MysqlAsync::_write_data\n";
    my ( $event, $event_type, $self ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::_write_data: not connected!\n";
        return;
    }
    delete $connect->{write_event_added};
    my $current_request = $connect->{current_request};
    #print STDERR "MysqlAsync::_write_data, requests: ".Dumper($connect->{requests});
    unless( $current_request ) {
        do {
            $current_request = $connect->{current_request} = @{$self->{requests}} && $self->{requests}->[0];
            unless( $current_request ) {
                $connect->{write_event}->remove();
                return;
            }
            $current_request->{offset} = 0;
            $current_request->{tries} ||= 0;
            
            #print STDERR "MysqlAsync: requests:".Dumper( $connect->{requests} );
            
            if( $current_request->{tries} < MAX_REQUEST_TRIES ) {
                $current_request->{tries}++;
            } else {
                shift @{$self->{requests}};
                print STDERR "MysqlAsync: request[$current_request->{query}] made too many tries(".MAX_REQUEST_TRIES.") at ".$self->dsn()."\n";
                my $error = {
                    errno => ERRNO_NO_MORE_TRIES,
                    message => "MysqlAsync: too many tries\n",
                };
                #$current_request->{event}->remove() if $current_request->{event};
                delete $self->{pending_requests}->{$current_request->{id}};
                AsyncCaller::schedule(\&_handle_failed_request, $self, $current_request->{callback}, $current_request->{callback_param}, $error ) 
                    if $current_request->{callback};
                $current_request =  undef;
            }
            
        } while( !$current_request );
        $connect->{db_request_started} = time();
    }
    #my ( $query, $data ) = @{$connect->{current_request}};
    #print STDERR "MysqlAsync::_write_data, query: [$connect->{current_request}->[0]]\n";
    my $sent = syswrite( $connect->{sock}, $current_request->{data}, $current_request->{length} - $current_request->{offset}, $current_request->{offset} );
    #print STDERR "MysqlAsync::_write_data [$sent] bytes sent\n";
    if( !$sent ) {
        return $self->error_reconnect( "MysqlAsync::_write_data server closed connection\n" );
    }
    $current_request->{offset} += $sent;
    
    #print STDERR "MysqlAsync::_write_data, current_request: ".Dumper($connect->{current_request});
    if( $current_request->{offset} >= $current_request->{length} ) {
        $self->can_read(1);
        delete $connect->{current_request};
        #print STDERR "MysqlAsync::_write_data: query [$current_request->{query}] sent\n";
    } else {
        $self->_add_write_event();
    }
}

sub _check_db_timeout
{
    my ( $self ) = @_;
    my $connect = $self->{connect};
    return unless $connect;
    if( @{$self->{requests}} && $connect->{db_request_started} && time() - $connect->{db_request_started} > $self->{db_timeout} ) {
        my $request = shift @{$self->{requests}};
        print STDERR "MysqlAsync: db timeout[$request->{query}], $self->{db_timeout} sec. at ".$self->dsn()."\n";
        my $error = {
            errno => ERRNO_DB_TIMIOUT,
            message => "MysqlAsync: db timeout\n",
        }; 
        #$request->{event}->remove() if $request->{event};
        delete $self->{pending_requests}->{$request->{id}};
        $self->_handle_failed_request( $request->{callback}, $request->{callback_param}, $error );
        $self->error_reconnect( "MysqlAsync: db timeout\n" ); 
    }
}


sub can_write
{
    my ( $self, $can ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::can_write: not connected!\n";
        return;
    }
    #print STDERR "MysqlAsync::can_write, can: ".Dumper($can)."\n";
    if( defined( $can ) && $can ) {
        $connect->{state} = 'write';        
        $connect->{read_event}->remove();
        #print STDERR "MysqlAsync::can_write, connect=".Dumper( $connect );
        #print STDERR "MysqlAsync::can_write, pending=".Dumper( $connect->{write_event}->pending() );
        #$self->_add_write_event();
        $connect->{greeting} ? _write_data( undef, undef, $self ) : $self->_add_write_event();
#        if( @{$connect->{requests}} && !$connect->{write_event}->pending() ) {
#            #print STDERR "MysqlAsync::can_write: adding write_event\n";
#            $connect->{write_event}->add(); 
#        }
    } else {
        return $connect->{state} && ( $connect->{state} eq 'write' );
    }
}    
    
sub _add_write_event
{
    my ( $self ) = @_;
    my $connect = $self->{connect};
    return unless $connect;
    if( !$connect->{write_event_added} ) {
        $connect->{write_event_added} = 1;
        $connect->{write_event}->add();
    }
}    


sub can_read
{
    my ( $self, $can ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::can_read: not connected!\n";
        return;
    }
    if( defined( $can ) && $can ) {
        $connect->{state} = 'read';        
        $connect->{write_event}->remove();
        $connect->{read_event}->add();
        $connect->{parsed_offset} = 0;
        $connect->{data} = '';
    } else {
        return $connect->{state} && ( $connect->{state} eq 'read' );
    }
}    
    
   
sub _read_data
{
    my ( $event, $event_type, $self ) = @_;
    $self->_mysql_data( $event->fh() );
}    

sub _mysql_data
{
    #print STDERR "MysqlAsync::_mysql_data\n";
    my ( $self, $sock ) = @_;
    my $connect = $self->{connect};
    my $len = length $connect->{data};
    #my $nread = sysread( $event->fh(), $data, 2048 );
    my $nread = sysread( $sock, $connect->{data}, 2*65535, $len );
    if( $nread ) {
        my $add_read = 0;
        my $failed_tries = 0;
        do {
           #print stderr "mysqlasync: starting add_read\n";
            my $len = length $connect->{data};
            $add_read = sysread( $sock, $connect->{data}, 2*65535, $len );
            $nread += $add_read if defined $add_read;
            #print STDERR "MysqlAsync: \$add_read=$add_read\n";
        } while( $add_read );    
        #print STDERR "MysqlAsync: $nread bytes read\n";
        my $action = undef;
        #print STDERR "MysqlAsync::_mysql_data before do\n";
        do{
            #print STDERR "MysqlAsync::_mysql_data calling _process_mysql_data\n";
            $action = $self->_process_mysql_data();
        } while( $action == NEED_REDO ); 
    } else {
        #print STDERR "MysqlAsync: nread is bad:".Dumper( $nread, $! );
        my $database = $self->{database};
        return $self->error_reconnect( "MysqlAsync: server $database->{host}:$database->{port} disconnected\n" );
    }
}

sub _process_mysql_data
{
    my ( $self ) = @_;
    my $conn = $self->{connect};
    #print STDERR "MysqlAsync::_process_mysql_data: len = ".length($conn->{data})."\n";
    #my $hex = unpack( "H*", $conn->{data});
    #substr( $hex, $conn->{parsed_offset}*2, 0, '->' );
    #print STDERR "MysqlAsync::_process_mysql_data: data = [$hex]\n";
    $conn->{db_request_started} = time();
    if ( !$conn->{packet} ) {
        #print STDERR ">>packet off=$conn->{parsed_offset}\n";
        delete $conn->{error};
        my $packet = {};
        my $rc = mysql_decode_header( $packet, $conn->{data}, $conn->{parsed_offset} );
        if ($rc < 0) {
            return $self->error_reconnect( "MysqlAsync: bad header\n" );
        }
        elsif ($rc > 0) {
            $conn->{packet} = $packet;
            #mysql_debug_packet( $conn->{packet} );
            return NEED_REDO;
        }
    } elsif ( !$conn->{greeting} ) {
        #print STDERR ">>greeting, off=$conn->{parsed_offset}\n";
        
        my $rc = mysql_decode_greeting( $conn->{packet}, $conn->{data}, $conn->{parsed_offset} );
        if ($rc < 0) {
            return $self->error_reconnect( "MysqlAsync: bad greeting\n" );
        }
        elsif ($rc > 0) {
            #mysql_debug_packet( $conn->{packet} );
            my $greeting = $conn->{greeting} = delete $conn->{packet};
            $self->_send_client_auth();
            $self->can_write(1);
            return NEED_REDO; 
        }
    } elsif ( !$conn->{result} ) {
        #print STDERR ">>result off=$conn->{parsed_offset}\n";
        my $packet = $conn->{packet};
        #print STDERR "MysqlAsync: packet: [".Dumper( $conn->{data} )."]\n";
#         #print STDERR "MysqlAsync: \$_: [".Dumper( $_ )."]\n";
        my $rc = mysql_decode_result( $packet, $conn->{data}, $conn->{parsed_offset} );
        if ($rc < 0) {
            #mysql_debug_packet( $conn->{packet} );
            return $self->error_reconnect( "MysqlAsync: bad result $rc\n" );
        } elsif ($rc > 0) {
            if ($packet->{error}) {
                #return $self->error_reconnect( "MysqlAsync: the server hates me\n" );
                $conn->{error} = {
                    errno    => $packet->{errno},
                    message  => $packet->{message}, 
                };
                #print STDERR "MysqlAsync: error $packet->{errno} at ".$self->dsn().": $packet->{message}\n";
                $self->_handle_response();                
                $self->can_write(1);
                
            } elsif ($packet->{end}) {
                return $self->error_reconnect( "MysqlAsync: this should never happen\n" );
            } else {
                if ($packet->{field_count}) {
                    #print STDERR "MysqlAsync: field_count = $packet->{field_count}\n";
                    $conn->{result} = $packet;
                    # fields and rows to come
                }
                elsif (!($packet->{server_status} & SERVER_MORE_RESULTS_EXISTS)) {
                    # that's that..
                    #mysql_debug_packet( $packet );
                    
                    $conn->{last_insert_id} = $packet->{last_insert_id}
                        if exists $packet->{last_insert_id};
                    $conn->{affected_rows} = $packet->{affected_rows}
                        if exists $packet->{affected_rows};
                    $self->_handle_response();                
                    $self->can_write(1);
#                     $conn->{data} = $_;
#                     return NEED_DATA;
                }
            }
            delete $conn->{packet};
            return NEED_REDO;
        }
    } elsif ( !$conn->{field_end} ) {
        #print STDERR ">>field\n";
        my $packet = $conn->{packet};
        my $rc = mysql_test_var( $packet, $conn->{data}, $conn->{parsed_offset} ) 
            ? mysql_decode_field( $packet, $conn->{data}, $conn->{parsed_offset} )
            : mysql_decode_result( $packet, $conn->{data}, $conn->{parsed_offset} );
        if ($rc < 0) {
            return $self->error_reconnect( "MysqlAsync: bad field packet\n" );
        }
        elsif ($rc > 0) {
            #mysql_debug_packet( $packet );
            if ($packet->{error}) {
                return $self->error_reconnect( "MysqlAsync: the server hates me\n" );
            }
            elsif ($packet->{end}) {
                $conn->{field_end} = $packet;
            }
            else {
                $conn->{response}->{fields} ||= [];
                push @{$conn->{response}->{fields}}, $packet->{name};
                #print STDERR "MysqlAsync: field meta data ready\n";
            }
            delete $conn->{packet};
            return NEED_REDO;
        }
    } else {
        #print STDERR ">>rows\n";
        my $packet = $conn->{packet};
        my $rc = mysql_test_var( $packet, $conn->{data}, $conn->{parsed_offset} ) 
            ? mysql_decode_row( $packet, $conn->{data}, $conn->{parsed_offset} )     
            : mysql_decode_result( $packet, $conn->{data}, $conn->{parsed_offset} ); 
        if ($rc < 0) {
            return $self->error_reconnect( "MysqlAsync: bad row packet\n" );
        }
        elsif ($rc > 0) {
            #mysql_debug_packet( $packet );
            if ($packet->{error}) {
                return $self->error_reconnect( "MysqlAsync: the server hates me\n" );
            } elsif ($packet->{end}) {
                delete $conn->{result};
                delete $conn->{field_end};
                unless ($packet->{server_status} & SERVER_MORE_RESULTS_EXISTS) {
                
                    $self->_handle_response();                
                    # that's that..
                    #mysql_debug_packet( $packet );
                    $self->can_write(1);
                    #return NEED_DATA;
                }
            } else {
                $conn->{response}->{rows} ||= [];
                push @{$conn->{response}->{rows}}, $packet->{row};
                #print STDERR "MysqlAsync: row is ready: ", Dumper($row), "\n";
            }
            delete $conn->{packet};
            return NEED_REDO;
        }
    }
    return NEED_DATA;   
}

sub _handle_response
{
    #print STDERR "MysqlAsync::_handle_response\n";
    my ( $self ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::_handle_response: not connected!\n";
        return;
    } 
    return unless @{$self->{requests}};
    #print STDERR Dumper( $connect->{requests} );
    my $req_time = time() - delete $connect->{db_request_started};
    my $request = shift @{$self->{requests}};
    my $query = $request->{query};
    if( $query ne 'raw_data' ) {
        $query=~s/[\n\r]/ /gso; 
        $self->log("%d;%.4g;%s\n", time(), $req_time, $query );
    }
    #$request->{event}->remove() if $request->{event};
    delete $self->{pending_requests}->{$request->{id}} if $request->{id};
    my ( $callback, $callback_param ) = @$request{qw/callback callback_param/};
    #print STDERR Dumper( $connect->{response} );
    if( $callback ) {
        my $result = undef;
        my $response = delete $connect->{response};
        if( !$connect->{error} ) {
            $result = [ $response->{fields} || [], $response->{rows} || [] ];
        }
        $callback->( $result, $callback_param );
    }
}

sub last_insert_id
{
    my ( $self ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::_handle_response: not connected!\n";
        return;
    } 
    return $connect->{last_insert_id};
}

sub affected_rows
{
    my ( $self ) = @_;
    my $connect = $self->{connect};
    unless( $connect ) {
        print STDERR "MysqlAsync::_handle_response: not connected!\n";
        return;
    } 
    return $connect->{affected_rows};
}


sub query_ex
{
    my ( $self, $unpack_func, $query, $callback, $callback_param, $timeout ) = @_;
    return $callback 
        ? $self->query( $query, \&_query_ex_callback, [$self, $unpack_func, $callback, $callback_param], $timeout )
        : $self->query( $query );
}

sub _query_ex_callback
{
    my ( $result, $params ) = @_;
    my ( $self, $unpack_func_alias, $callback, $callback_param ) = @$params;
    my $unpack_func = $self->RESPONSE_UNPACK_FUNC->{$unpack_func_alias};
    unless( $unpack_func ) {
        print STDERR "MysqlAsync::_query_ex_callback: unknown unpack function alias: ".Dumper( $unpack_func_alias );
    }
    if( defined $result ) {
        $callback->( $self->$unpack_func( @$result ), $callback_param );
    } else {
        $callback->( undef, $callback_param );
    }
}

sub insert
{
    my ( $self ) = ( shift );
    return $self->query_ex( insert => @_ );
}

sub update
{
    my ( $self ) = ( shift );
    return $self->query_ex( update => @_ );
}

sub delete
{
    my ( $self ) = ( shift );
    return $self->query_ex( delete => @_ );
}

sub execute
{
    my ( $self ) = ( shift );
    return $self->query_ex( execute => @_ );
}









sub get_array
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_array => @_ );
}

sub get_plain_array
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_plain_array => @_ );
}

sub get_hash
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_hash => @_ );
}

sub get_plain_hash
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_plain_hash => @_ );
}

sub get_row
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_row => @_ );
}

sub get_plain_row
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_plain_row => @_ );
}

sub get_scalar
{
    my ( $self ) = ( shift );
    return $self->query_ex( get_scalar => @_ );
}

sub _get_array_unpack
{
    my ( $self, $fields, $rows ) = @_;
    return @$rows 
        ? [ map { my $row = $_; ({ map { $fields->[$_] => $row->[$_] } 0..$#$fields }) } @$rows ]
        : 0;        
}

sub _get_plain_array_unpack
{
    my ( $self, $fields, $rows ) = @_;
    return @$rows && $rows;
}

sub _get_hash_unpack
{
    my ( $self, $fields, $rows ) = @_;
    return @$rows 
        ? { map { my $row = $_; ( $row->[0] => { map { $fields->[$_] => $row->[$_] } 0..$#$fields }) } @$rows }
        : 0;
}

sub _get_plain_hash_unpack
{
    my ( $self, $fields, $rows ) = @_;
    return @$rows 
        ? { map { @$_[0..1] } @$rows }
        : 0;
}

sub _get_row_unpack
{
    my ( $self, $fields, $rows ) = @_;
    my $row = @$rows && $rows->[0];
    return $row
        ? { map { $fields->[$_] => $row->[$_] } 0..$#$fields } 
        : 0;
}

sub _get_plain_row_unpack
{
    my ( $self, $fields, $rows ) = @_;
    return @$rows && $rows->[0];
}

sub _get_scalar_unpack
{
    my ( $self, $fields, $rows ) = @_;
    my $row = @$rows && $rows->[0];
    return $row && @$row 
        ? \$row->[0]
        : 0;
}

sub _insert_unpack
{
    my ( $self ) = @_;
    return $self->last_insert_id();
}

sub _update_unpack
{
    my ( $self ) = @_;
    return $self->affected_rows();
}

1;

}

{
package MysqlAsyncPool;
use strict;
use MysqlAsync;

sub new
{
    my ( $caller, $count, $args ) = @_;
    my $self = [ map { MysqlAsync->new(%$args) } 1..$count ];
    bless $self, $caller || (ref $caller);
}


sub query_ex
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->query_ex(@_);
}

sub insert
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->insert(@_);
}

sub update
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->update(@_);
}

sub delete
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->delete(@_);
}

sub execute
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->execute(@_);
}

sub get_array
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_array(@_);
}

sub get_plain_array
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_plain_array(@_);
}

sub get_hash
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_hash(@_);
}

sub get_plain_hash
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_plain_hash(@_);
}

sub get_row
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_row(@_);
}

sub get_plain_row
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_plain_row(@_);
}

sub get_scalar
{
    my $self = shift;
    my $dbh = $self->[rand @$self];
    return $dbh->get_scalar(@_);
}

sub quote
{
    my ($self, $statement) = @_;
    return 'NULL' unless defined $statement;

    for ($statement) {
        s/\\/\\\\/g;
        s/\0/\\0/g;
        s/\n/\\n/g;
        s/\r/\\r/g;
        s/'/\\'/g;
        s/"/\\"/g;
        s/\x1a/\\Z/g;
    }
    return "'$statement'";
}


1;

}


