# Connect to a given server and read initial data, simulating
# load balancer connecting to MySQL to check server availability.
# Following variables should be set:
# $SRV_HOST           Server hostname to connect to
# $SRV_PORT           Server port

--perl
  use Socket;

  my $host = $ENV{'SRV_HOST'};
  my $port = $ENV{'SRV_PORT'};
  my $iaddr = inet_aton($host) || die "no host: $host\n";
  my $paddr = sockaddr_in($port, $iaddr);
  my $proto = getprotobyname("tcp");

  socket(my $sock, PF_INET, SOCK_STREAM, $proto) || die "socket failed\n";
  connect($sock, $paddr) || die "connect failed to $host:$port\n";

  my $read = '';
  vec($read, fileno($sock), 1) = 1;  # Set the appropriate bit
  my $found = select($read, undef, undef, undef);

  # Does socket have data waiting?
  if (vec($read,fileno($sock), 1))
  {
     sysread($sock,my $line,1024);
  }

  print "closing connection\n";
  close($sock);
EOF
