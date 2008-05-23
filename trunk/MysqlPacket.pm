# based on MySQL::Packet module http://search.cpan.org/~tavin/MySQL-Packet-0.2007054/ by Tavin Cole
# but has many significant changes and optimisations 

package MysqlPacket;

BEGIN {
    eval 'use Digest::SHA qw(sha1); 1'      or
    eval 'use Digest::SHA1 qw(sha1); 1'     or die;
}

use Exporter qw(import);

our %EXPORT_TAGS = (
    COM => [qw/
        COM_SLEEP
        COM_QUIT
        COM_INIT_DB
        COM_QUERY
        COM_FIELD_LIST
        COM_CREATE_DB
        COM_DROP_DB
        COM_REFRESH
        COM_SHUTDOWN
        COM_STATISTICS
        COM_PROCESS_INFO
        COM_CONNECT
        COM_PROCESS_KILL
        COM_DEBUG
        COM_PING
        COM_TIME
        COM_DELAYED_INSERT
        COM_CHANGE_USER
        COM_BINLOG_DUMP
        COM_TABLE_DUMP
        COM_CONNECT_OUT
        COM_REGISTER_SLAVE
        COM_STMT_PREPARE
        COM_STMT_EXECUTE
        COM_STMT_SEND_LONG_DATA
        COM_STMT_CLOSE
        COM_STMT_RESET
        COM_SET_OPTION
        COM_STMT_FETCH
    /],
    CLIENT => [qw/
        CLIENT_LONG_PASSWORD
        CLIENT_FOUND_ROWS
        CLIENT_LONG_FLAG
        CLIENT_CONNECT_WITH_DB
        CLIENT_NO_SCHEMA
        CLIENT_COMPRESS
        CLIENT_ODBC
        CLIENT_LOCAL_FILES
        CLIENT_IGNORE_SPACE
        CLIENT_PROTOCOL_41
        CLIENT_INTERACTIVE
        CLIENT_SSL
        CLIENT_IGNORE_SIGPIPE
        CLIENT_TRANSACTIONS
        CLIENT_RESERVED
        CLIENT_SECURE_CONNECTION
        CLIENT_MULTI_STATEMENTS
        CLIENT_MULTI_RESULTS
    /],
    SERVER => [qw/
        SERVER_STATUS_IN_TRANS
        SERVER_STATUS_AUTOCOMMIT
        SERVER_MORE_RESULTS_EXISTS
        SERVER_QUERY_NO_GOOD_INDEX_USED
        SERVER_QUERY_NO_INDEX_USED
        SERVER_STATUS_CURSOR_EXISTS
        SERVER_STATUS_LAST_ROW_SENT
        SERVER_STATUS_DB_DROPPED
        SERVER_STATUS_NO_BACKSLASH_ESCAPES
    /],
    debug => [qw/
        mysql_debug_packet
    /],
    test => [qw/
        mysql_test_var
        mysql_test_end
        mysql_test_error
    /],
    decode => [qw/
        mysql_decode_header
        mysql_decode_skip
        mysql_decode_varnum
        mysql_decode_varstr
        mysql_decode_greeting
        mysql_decode_result
        mysql_decode_field
        mysql_decode_row
    /],
    encode => [qw/
        mysql_encode_header
        mysql_encode_varnum
        mysql_encode_varstr
        mysql_encode_client_auth
        mysql_encode_com_query
    /],
    crypt => [qw/
        mysql_crypt
    /],
);

our @EXPORT_OK = map {@$_} values %EXPORT_TAGS;

use constant {
    COM_SLEEP                   => 0x00,
    COM_QUIT                    => 0x01,
    COM_INIT_DB                 => 0x02,
    COM_QUERY                   => 0x03,
    COM_FIELD_LIST              => 0x04,
    COM_CREATE_DB               => 0x05,
    COM_DROP_DB                 => 0x06,
    COM_REFRESH                 => 0x07,
    COM_SHUTDOWN                => 0x08,
    COM_STATISTICS              => 0x09,
    COM_PROCESS_INFO            => 0x0a,
    COM_CONNECT                 => 0x0b,
    COM_PROCESS_KILL            => 0x0c,
    COM_DEBUG                   => 0x0d,
    COM_PING                    => 0x0e,
    COM_TIME                    => 0x0f,
    COM_DELAYED_INSERT          => 0x10,
    COM_CHANGE_USER             => 0x11,
    COM_BINLOG_DUMP             => 0x12,
    COM_TABLE_DUMP              => 0x13,
    COM_CONNECT_OUT             => 0x14,
    COM_REGISTER_SLAVE          => 0x15,
    COM_STMT_PREPARE            => 0x16,
    COM_STMT_EXECUTE            => 0x17,
    COM_STMT_SEND_LONG_DATA     => 0x18,
    COM_STMT_CLOSE              => 0x19,
    COM_STMT_RESET              => 0x1a,
    COM_SET_OPTION              => 0x1b,
    COM_STMT_FETCH              => 0x1c,
};

use constant {
    CLIENT_LONG_PASSWORD        =>  1,
    CLIENT_FOUND_ROWS           =>  2,
    CLIENT_LONG_FLAG            =>  4,
    CLIENT_CONNECT_WITH_DB      =>  8,
    CLIENT_NO_SCHEMA            =>  16,
    CLIENT_COMPRESS             =>  32,
    CLIENT_ODBC                 =>  64,
    CLIENT_LOCAL_FILES          =>  128,
    CLIENT_IGNORE_SPACE         =>  256,
    CLIENT_PROTOCOL_41          =>  512,
    CLIENT_INTERACTIVE          =>  1024,
    CLIENT_SSL                  =>  2048,
    CLIENT_IGNORE_SIGPIPE       =>  4096,
    CLIENT_TRANSACTIONS         =>  8192,
    CLIENT_RESERVED             =>  16384,
    CLIENT_SECURE_CONNECTION    =>  32768,
    CLIENT_MULTI_STATEMENTS     =>  65536,
    CLIENT_MULTI_RESULTS        =>  131072,
};

use constant {
    SERVER_STATUS_IN_TRANS              => 1,
    SERVER_STATUS_AUTOCOMMIT            => 2,
    SERVER_MORE_RESULTS_EXISTS          => 8,
    SERVER_QUERY_NO_GOOD_INDEX_USED     => 16,
    SERVER_QUERY_NO_INDEX_USED          => 32,
    SERVER_STATUS_CURSOR_EXISTS         => 64,
    SERVER_STATUS_LAST_ROW_SENT         => 128,
    SERVER_STATUS_DB_DROPPED            => 256,
    SERVER_STATUS_NO_BACKSLASH_ESCAPES  => 512,
};

sub mysql_debug_packet {
    my $packet = $_[0];
    my $stream = $_[1] || \*STDERR;
    while (my ($k, $v) = each %$packet) {
        if ($k eq 'row') {
            $v = "@$v";
        }
        elsif ($k eq 'server_capa') {
            $v = sprintf('%16b', $v) . ' = ' . join ' | ', grep { $v & eval } @{ $EXPORT_TAGS{CLIENT} };
        }
        elsif ($k eq 'crypt_seed') {
            $v = unpack 'H*', $v;
        }
        print $stream "$k\t=>\t$v\n";
    }
    return;
}

sub mysql_test_var {
    length($_[1]) && do {
        my $x = ord substr $_[1], $_[2], 1;
        $x <= 0xfd ||
        $x == 0xfe && $_[0]{packet_size} >= 9
    };
}

sub mysql_test_end {
    length($_[1]) && ord( substr $_[1], $_[2], 1 )  == 0xfe && $_[0]{packet_size} < 9;
}

sub mysql_test_error {
    length($_[1]) && ord( substr $_[1], $_[2], 1 ) == 0xff;
}

sub mysql_decode_header {
    return 0 unless $_[2] + 4 <= length $_[1];
    my $header = unpack 'V', substr $_[1], $_[2], 4;
    $_[2]+=4;
    $_[0]{packet_size} = $header & 0xffffff;
    $_[0]{packet_serial} = $header >> 24;
    return 4;
}

sub mysql_decode_varnum {
    my $len = length $_[1];
    return 0 unless $_[2] <= $len;
    my $first = ord substr $_[1], $_[2], 1;
    if ($first < 251) {
        $_[0] = $first;
        $_[2] ++;
        return 1;
    }
    elsif ($first == 251) {
        $_[0] = undef;
        $_[2] ++;
        return 1;
    }
    elsif ($first == 252) {
        return 0 unless 3 + $_[2] <= $len;
        $_[2] ++;
        $_[0] = unpack 'v', substr $_[1], $_[2], 2;
        $_[2] += 2;
        return 3;
    }
    elsif ($first == 253) {
        return 0 unless 5 + $_[2] <= $len;
        $_[2] ++;
        $_[0] = unpack 'v', substr $_[1], $_[2], 4;
        $_[2] += 4;
        return 5;
    }
    elsif ($first == 254) {
        return 0 unless 9 + $_[2] <= $len;
        $_[2] ++;
        $_[0] = (unpack 'V', substr $_[1], $_[2], 4 )
              | (unpack 'V', substr $_[1], $_[2] + 4, 4 ) << 32;
        $_[2] += 8;
        return 9;
    }
    else {
        return -1;
    }
}

sub mysql_decode_varstr {
    my $length;
    my $i = mysql_decode_varnum $length, $_[1], $_[2];
    #print STDERR "mysql_decode_varstr: string len: $length, ".substr($_[1], $_[2], 1)."\n";
    if ($i <= 0) {
        $i;
    }
    elsif (not defined $length) {
        #$_[2] += $i;
        $_[0] = undef;
        $i;
    }
    elsif ( $_[2] + $length <= length $_[1] ) {
        #$_[2] += $i;
        $_[0] = substr $_[1], $_[2], $length;
        $_[2] += $length;
        $i + $length;
    }
    else {
        0;
    }
}

sub mysql_decode_greeting {
    return 0 unless $_[0]{packet_size} + $_[2] <=  length $_[1];
    # todo: older protocol doesn't include 2nd crypt_seed fragment..
    (
        $_[0]{protocol_version},
        $_[0]{server_version},
        $_[0]{thread_id},
        my $crypt_seed1,
        $_[0]{server_capa},
        $_[0]{server_lang},
        $_[0]{server_status},
        my $crypt_seed2,
    ) = eval {
        unpack 'CZ*Va8xvCvx13a12x', substr $_[1], $_[2], $_[0]{packet_size}
    };
    $_[2] += $_[0]{packet_size};
               
    return -1 if $@;    # believe it or not, unpack can be fatal..
                        # todo: investigate performance penalty of eval;
                        #       this could be replaced with cautious logic
    $_[0]{crypt_seed} = $crypt_seed1 . $crypt_seed2;
    return $_[0]{packet_size};
}


sub mysql_decode_result {
    my $n = $_[0]{packet_size};
    return 0 unless $n + $_[2] <= length $_[1];
    return do {
        my $type = ord substr $_[1], $_[2], 1;
        #print STDERR "type: $type\n";
        if ($type == 0xff) {
            return -1 unless 3 <= $n;
            $_[2]++;
            $_[0]{error} = 1;
            $_[0]{errno} = unpack 'v', substr $_[1], $_[2], 2;
            $_[2]+=2;
            $_[0]{message} = substr $_[1], $_[2], $n - 3;
            $_[2]+= $n-3;
            $_[0]{sqlstate} = ($_[0]{message} =~ s/^#// ? substr $_[0]{message}, 0, 5, '' : '');
            $n;
        }
        elsif ($type == 0xfe && $n < 9) {
            if ($n == 1) {
                $_[2]++;
                $_[0]{end} = 1;
                $_[0]{warning_count} = 0;
                $_[0]{server_status} = 0;
                1;
            }
            elsif ($n == 5) {
                $_[2]++;
                $_[0]{end} = 1;
                $_[0]{warning_count} = unpack 'v', substr $_[1], $_[2], 2;
                $_[2]+=2;
                $_[0]{server_status} = unpack 'v', substr $_[1], $_[2], 2;
                $_[2]+=2;
                5;
            }
            else {
                return -1;
            }
        }
        elsif ($type > 0) {
            my $i = 0;
            my $j;
            0 >= ($j = mysql_decode_varnum $_[0]{field_count}, $_[1], $_[2]) ? (return -1) : ($i += $j);
            0 >= ($j = mysql_decode_varnum $_[0]{extra}, $_[1], $_[2]) ? (return -1) : ($i += $j) if $i < $n;
            $i;
        }
        else {
            $_[2]++;
            my $i = 1;
            my $j;
            $_[0]{field_count} = 0;
            0 >= ($j = mysql_decode_varnum $_[0]{affected_rows},$_[1], $_[2]) ? (return -1) : ($i += $j);
            0 >= ($j = mysql_decode_varnum $_[0]{last_insert_id}, $_[1], $_[2]) ? (return -1) : ($i += $j);
            $_[0]{server_status} = unpack 'v', substr $_[1], $_[2], 2;
            $i += 2;
            $_[2]+=2;
            # todo: older protocol has no warning_count here
            $_[0]{warning_count} = unpack 'v', substr $_[1], $_[2], 2; $i += 2;
            $_[2]+=2;
            0 >= ($j = mysql_decode_varstr $_[0]{message}, $_[1], $_[2]) ? (return -1) : ($i += $j) if $i < $n;
            $i > $n ? $n : $i ;
        }
    } == $n ? $n : -7;
}

sub mysql_decode_field {
    return 0 unless $_[0]{packet_size} + $_[2] <= length $_[1];
    my $i = 0;
    my $j;
    0 >= ($j = mysql_decode_varstr $_[0]{catalog}, $_[1], $_[2])      ? (return -1) : ($i += $j);
    0 >= ($j = mysql_decode_varstr $_[0]{db}, $_[1], $_[2])           ? (return -1) : ($i += $j);
    0 >= ($j = mysql_decode_varstr $_[0]{table}, $_[1], $_[2])        ? (return -1) : ($i += $j);
    0 >= ($j = mysql_decode_varstr $_[0]{org_table}, $_[1], $_[2])    ? (return -1) : ($i += $j);
    0 >= ($j = mysql_decode_varstr $_[0]{name}, $_[1], $_[2])         ? (return -1) : ($i += $j);
    0 >= ($j = mysql_decode_varstr $_[0]{org_name}, $_[1], $_[2])     ? (return -1) : ($i += $j);
    $_[2]++; $i += 1;
    $_[0]{charset_no} = unpack 'v', substr $_[1], $_[2], 2; $i += 2;
    $_[2]+=2;
    $_[0]{display_length} = unpack 'V', substr $_[1], $_[2], 4; $i += 4;
    $_[2]+=4;
    $_[0]{field_type} = ord substr $_[1], $_[2], 1; $i += 1;
    $_[2]+=1;
    $_[0]{flags} = unpack 'v', substr $_[1], $_[2], 2; $i += 2;
    $_[2]+=2;
    $_[0]{scale} = ord substr $_[1], $_[2], 1; $i += 1;
    $_[2]+=1;
    $i += 2;
    $_[2]+=2;
    
   $i == $_[0]{packet_size} ? $i : -1;
}

sub mysql_decode_row {
    return 0 unless $_[0]{packet_size} + $_[2] <= length $_[1];
    my ($n, $i, $j);
    for ($n = 0, $i = 0; $i < $_[0]{packet_size}; $i += $j) {
        return -1 if 0 >= ($j = mysql_decode_varstr $_[0]{row}[$n++], $_[1], $_[2]);
    }
    $i == $_[0]{packet_size} ? $i : -1;
}

sub mysql_encode_header {
    pack 'V', length($_[0]) | (exists $_[1] ? $_[1] << 24 : 0);
}

sub mysql_encode_varnum {
    my $num = $_[0];
    $num <= 250         ? chr($num)                     :
    $num <= 0xffff      ? chr(252) . pack('v', $num)    :
    $num <= 0xffffffff  ? chr(253) . pack('V', $num)    :
    chr(254) . pack('V', $num & 0xffffffff) . pack('V', $num >> 32);
}

sub mysql_encode_varstr {
    map { mysql_encode_varnum(length) . $_ } @_;
}

sub mysql_encode_client_auth {
    my (
        $flags,
        $max_packet_size,
        $charset_no,
        $username,
        $crypt_pw,
        $database,
    ) = @_;
    my $packet = pack 'VVCx23Z*a*', (
        $flags,
        $max_packet_size,
        $charset_no,
        $username,
        mysql_encode_varstr($crypt_pw),
    );
    $packet .= pack 'Z*', $database if $flags & CLIENT_CONNECT_WITH_DB;
    return $packet;
}

sub mysql_encode_com_quit {
    chr(COM_QUIT);
}

sub mysql_encode_com_query {
    chr(COM_QUERY) . join '', @_;
}

sub mysql_crypt {
    my ($pass, $salt) = @_;
    my $crypt1 = sha1 $pass;
    my $crypt2 = sha1 ($salt . sha1 $crypt1);
    return $crypt1 ^ $crypt2;
}

1;


