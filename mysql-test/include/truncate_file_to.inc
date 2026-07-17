# ==== Purpose ====
#
# Truncate a given file to a given position.
#
# ==== Usage ====
#
# --let $file = /path/to/file
# --let $truncate_pos = 1234
# --source include/truncate_file_to.inc

if (!$file)
{
  --echo Please assign a file name to \$file!!
  exit;
}

if (!$truncate_pos)
{
  --echo Please assign a postion to \$truncate_pos!!
  exit;
}

let TRUNCATE_FILE= $file;
let TRUNCATE_POS= $truncate_pos;

perl;
my $truncate_pos = $ENV{'TRUNCATE_POS'};
my $filename = $ENV{'TRUNCATE_FILE'};
$filename =~ s/\/\//\//;
truncate $filename, $truncate_pos || "Failed to truncate file $filename to pos $truncate_pos"
EOF
