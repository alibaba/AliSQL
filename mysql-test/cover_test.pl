#!/usr/bin/perl -w

use strict;
use FileHandle;
use Getopt::Std;
use IO::File;
use POSIX qw(strftime);

format HEADER =

 cover_test.pl -f diff.log -d ./
    Syntax:  cover_test.pl -option value 
      -h          - Give help screen
      -f          - diff file [required]
      -d          - directory [optional]
.

my $debug	=0;

my ($diff, $directory,$log);
&do_opt();

if(! defined $diff){
   &crit_record("pls input diff file");
}

if(!defined $directory){
   $directory	="./";
}

if(defined $log){
	print "pls check the output in [$directory/$log]\n";
	my $fd  =POSIX::open( "$directory/$log", &POSIX::O_CREAT | &POSIX::O_WRONLY, 0640);
	POSIX::dup2($fd, 1);
	POSIX::dup2($fd, 2);
}

&record("directory=[$directory]; diff=[$diff];");

#interpret the diff file

my($hd_diff)	=&getFileHandle($diff,"r");

if (! defined $hd_diff){
	&crit_record("diff [$diff] init fail, pls check it!");
}

&record("begin to interpret diff file!");

my $reg_diff		= 'diff --git [ab]/(.+?)[\s\n]';
my $reg_add		= '^\+(.*)';
my @reg_old		= ('^-');
my @reg_del		= ('^\+') ;


my @reg_file		= ('\.h$','\.cc$','\.c$','\.ic$');

my $type_gcno		= "gcno";
my $type_gcov		= "gcov";
my $reg_cnt		= '^@@.*?\d*,\d*.*?(\d*),(\d*) @@';
my $reg_gcov		= '^#####:\s*(\d+):(.*)';
my $reg_hgcov_file	= '\.h\.gcov';
my $reg_file_prefix	= '(.*)\..*';

my $line;
my %src_file	= ();
my ($ref_raw_line, $raw_line, $key);
my ($bcnt, $ecnt)	=(0,0);
my $delta		=-1;
my $current		=0;
my $in_block		=0;
while($line = $hd_diff->getline()){
	# find the file
	$ref_raw_line	= &ppgrep($line, $reg_diff, 1);
	if(defined $ref_raw_line){
		$in_block		= 0;
		$delta			= -1;
		$raw_line		= $ref_raw_line->[0];
		$raw_line		= "$directory/$raw_line";
		&debug_record("diff file has: $raw_line");
		my @add_lines		= ();
		$key			= $raw_line;
		$src_file{$key}		= \@add_lines;
		
		$bcnt			= 0;
		$ecnt			= 0;
		next;
	}

	$ref_raw_line	= &ppgrep($line, $reg_cnt, 2);
	if(defined $ref_raw_line){
		$in_block	= 1;
		$delta		= -1;
		my $c1		= $ref_raw_line->[0];
		my $c2		= $ref_raw_line->[1];
		$bcnt		= $c1;
		$ecnt		= $c1+$c2-1;
		$current	= $c1-1;
		print("\tdiff file has: $key: begin: $bcnt  end: $ecnt\n");
		next;
	}

	if ($in_block == 0){
		next;
	}
	my $is_old	= &filterLine(\@reg_old, $line);
	if ($is_old == 0){
		next;
	}
	$current	= $current+1;
	#find the lines with head of '+'
	$ref_raw_line	= &ppgrep($line, $reg_add, 1);
	if(defined $ref_raw_line){
		my $new_ref_array	= $src_file{$key};
		&update_ecnt_by_bcnt($new_ref_array, $bcnt);
		$raw_line		= $ref_raw_line->[0];
		$delta	= $delta+1;

		#del the line match the array:reg_del
		#my $is_del		= &filterLine(\@reg_del,$raw_line);
		#if( $is_del == 0){
		#	next;
		#}
		
		#del the backspace
		$raw_line		= &delbackspace($raw_line);
		if (! defined $raw_line){
			next;
		}
	        &debug_record("diff file lines: $raw_line");
		
		my $tmp_ref_array	= $src_file{$key};
		my @st_line		= ($raw_line,$bcnt, $bcnt+$delta);	
		push(@$tmp_ref_array,   \@st_line);
		next;
	}
	elsif (!defined $ref_raw_line)
        {
		$bcnt	= $current+1;
		$delta	= -1;
        }
}

#-------------------------------print the currect line scope---------------------------------

my $value;
my $repeat	= -1;
print "\n";
&record("begin to amend diff file[amend the bcnt, ecnt]!");
while(($key, $value)    = each %src_file){
	$repeat	= -1;
	foreach my $ref_correct(@$value){
		my ($line_correct, $bcnt_correct, $ecnt_correct)	=@$ref_correct;
			&debug_record("\tline:$line_correct|bcnt=$bcnt_correct|ecnt=$ecnt_correct\n");
		if ($repeat == $bcnt_correct){
			next;
		}
		else{
			$repeat	=$bcnt_correct;
			print("\tdiff file has: $key: begin: $bcnt_correct  end: $ecnt_correct\n");
		}
	}
}

print "\n";
#-------------------------------filter the not c/c++/h source file---------------------------------

my @filtered_file	= ();
my @valid_file		= ();
my $all_file_cnt	= 0;
while(($key, $value)	= each %src_file){
	$all_file_cnt++;
	# filter the file 
	my $ret		= &filterLine(\@reg_file, $key);
	if($ret == 0){
		push(@valid_file, $key);
		next;
	}
	else{
		push(@filtered_file, $key);
	}
}

my $novalid_cnt	= scalar(@filtered_file);
my $valid_cnt	= $all_file_cnt - $novalid_cnt;

&record("The diff log include : [$all_file_cnt\t] files");

&record("The valid source file: [$valid_cnt\t]; listing as below:\n");

my $add_lines_cnt;

foreach my $file(@valid_file){
	$value	= $src_file{$file};
	$add_lines_cnt	=scalar(@$value);
	print "\t\t\t lines:[$add_lines_cnt\t];file:[$file]\n";
}

&record("The filtered file: [$novalid_cnt\t]; listing as below:\n");

foreach my $file(@filtered_file){
        $value  = $src_file{$file};
        $add_lines_cnt  =scalar(@$value);
        print "\t\t\t lines:[$add_lines_cnt\t];file:[$file]\n";
}

#---------------------------------begin to check-----------------------------------------------------

&record("begin to check files:");

my ($file_name, $file_gcno,$file_name_gcno,  $file_gcov, $file_name_gcov);
foreach my $file(@valid_file){
	print("\nfile: $file-------------------------------------------------------\n");
	if (-e $file){
		#find the gcno file
		#
		$file_name	= &getFileName($file);
		#----------------debug
		if( $debug and $file_name ne "mysqld.cc"){
                        #next;
		}
		#----------------debug
		$file_name_gcno	= &genTargetFileName($file_name, $type_gcno);
		$file_gcno	= &find_gcno($file_name_gcno,$directory);
		&record("file=[$file];gcno=[$file_name_gcno]");
		if(!defined $file_gcno or ! -e $file_gcno){
			&warn_record("file [$file_name_gcno] not exists!");
			
			$file_name_gcno = &genTargetFileNameFor51($file_name, $type_gcno);
                	$file_gcno      = &find_gcno($file_name_gcno,$directory);
                	&record("file=[$file];gcno=[$file_name_gcno]");
                	if(!defined $file_gcno or ! -e $file_gcno){
                        	&warn_record("file [$file_name_gcno] not exists!");
			}
		}
		#gen the gcov file

		$file_name_gcov	= &genTargetFileName($file_name, $type_gcov);
		$file_gcov	= "$directory/$file_name_gcov";
                &debug_record("file_gcov: $file_gcov");
		my $ret		= &gengcov($file, $file_gcno);
		&record("file=[$file];gcov=[$file_gcov]");
		if ($ret != 0){
			&warn_record("file [$file_name_gcno--->$file_gcov] gen gcov error!");
			#next;
		}
		
		if( ! -e $file_gcov){
			&warn_record("file [$file_name_gcov] not exists, skip it!");
                        my $reg_h_file	= &isgrep($file_gcov, $reg_hgcov_file);
			if( $reg_h_file == 0){
				next;
			}else{
				my $str1="##### file [$file_name_gcov] not exists, exit cover test!!";
				&sp_warn_record($str1,"");
				exit(1);
			}
		}

		&record("......begin to scan gcov file [$file_gcov]......");	
		my $ref_diff_lines	=$src_file{$file};
		
		&compare($ref_diff_lines, $file_gcov);
	}
	else{
		&warn_record("file [$file] not exists!");
		next;
	}	

}


#------------------------------------------------------------
#correct the end line number
#------------------------------------------------------------
sub update_ecnt_by_bcnt(){
	my($ref_array, $bcnt)	= @_;
	if(defined $ref_array){
		foreach my $ref(@$ref_array){
			my($tmp_line, $tmp_bcnt, $tmp_ecnt)	=@$ref;
			if ($tmp_bcnt == $bcnt){
				@$ref	=($tmp_line, $tmp_bcnt, $tmp_ecnt+1);
			}
		}
		
	}
}

#------------------------------------------------------------
#find the file
#------------------------------------------------------------
sub find_gcno(){
	my($file_name_gcno, $dir)	=@_;
        &debug_record("find_gcno: find $dir -name '$file_name_gcno'");
	my @result			= `find $dir -name '$file_name_gcno'`;
	if(defined $result[0]){
		chomp($result[0]);
		return $result[0];
	}
	else{
		
		return undef;
	}
}

#----------------------------------------------------
#append the file type
#----------------------------------------------------
sub genTargetFileName(){
	my($file_name, $type)	= @_;
	my $type_file		= "$file_name.$type";
	return $type_file;
}


sub genTargetFileNameFor51(){
	my($file_name, $type)	= @_;
	my $ref_file_prefix	= &ppgrep($file_name,$reg_file_prefix,1);
	my $file_prefix		= $ref_file_prefix->[0];
	return "$file_prefix.$type";

}
#-----------------------------------------------------
#get the filename from string
#-----------------------------------------------------
sub getFileName(){
	my($full_path)	= @_;
	my $file_name;
	if($full_path	=~ /.*\/(.*?)$/){
		$file_name	= $1;
	}
	else{
		$file_name	= $full_path;
	}
	return $file_name;
}

#------------------------------------------------------------------------
#
#------------------------------------------------------------------------
sub compare(){
	my ($ref_diff_lines, $file_gcov)	=@_;

	my $hd_gcov	= &getFileHandle($file_gcov, "r");
	my $reg		= "^#####:";
        
	my $reg_no_cover	= "\/\/no cover line";
	my $reg_no_cover_begin	= "\/\/no cover begin";
	my $reg_no_cover_end	= "\/\/no cover end";
	
	my $no_cover_cnt	= 0;

	my $in_no_cover		= 0;
        
	my $bcnt;
	my $ecnt;
	my $sr_cnt;
	my $cov_ret;
	while(my $line	= $hd_gcov->getline()){
		
		my $parse_line	= $line;
		#&debug_record("the gcno lines: $parse_line");
	
		#step 1: del backspace
		$parse_line	= &delbackspace($parse_line);
		if( !defined $parse_line){
			next;
		}

		# no cover line
		$cov_ret	= &isgrep($parse_line, $reg_no_cover);
		if($cov_ret == 0){
			$no_cover_cnt++;
			next;
		}
		
		$cov_ret	= &isgrep($parse_line, $reg_no_cover_begin);
		if( $cov_ret == 0 and $in_no_cover == 0){
			$no_cover_cnt++;
			$in_no_cover=1;
			next;
		}
		
		$cov_ret	= &isgrep($parse_line, $reg_no_cover_end);
		if ($cov_ret ==0 and $in_no_cover ==1){
			$no_cover_cnt++;
			$in_no_cover=0;
			next;
		}

		if($in_no_cover == 1){
			$no_cover_cnt++;
			next;
		}
		

		#&debug_record("the gcno lines: $parse_line");

		#step 2: grep #####
		$parse_line	= &ppgrep($parse_line, $reg_gcov, 2); 
		
		if( !defined $parse_line){
			next;
		}
		$sr_cnt		= $parse_line->[0];
		$parse_line	= $parse_line->[1];
		#&debug_record("the gcno source lines: $line");

		#step 3: del backspace
		$parse_line		= &delbackspace($parse_line);
		if(!defined $parse_line){
			next;
		}
		
		#&debug_record("the gcno delbak lines: $parse_line");
		#step 4: del the no meaning line
		my $ret		= &ismeaning($parse_line);
		if( $ret != 0){
			next;	
		}
		
		#&debug_record("the gcno is meaning lines: $parse_line");
		#print "$line\n"	;
		foreach my $diff_line(@$ref_diff_lines){
			$ret	= &isMatchStr($parse_line, $sr_cnt, $diff_line);
			&debug_record("begin: $line  end: $diff_line : result is $ret");
			if($ret	== 0){
				print "\t\t\t$line";
				last;
			}
		}
	}

	if($no_cover_cnt>0)
	{
		my $str2	="";
		my $str1	= "filter the no cover lines: $no_cover_cnt";
		if($in_no_cover != 0){
			$str2="##### crit: no cover comment is not pairing, pls check!";
		}
		&sp_warn_record($str1, $str2);
		
		if($in_no_cover != 0){
			exit(1);
		}	
	}
}

#------------------------------------------------------------------
#judge it is meaning line
#------------------------------------------------------------------
sub ismeaning(){
	my($str)	= @_;
	if( $str =~ /[0-9]|[a-z]|[A-Z]/){
		return 0;
	}
	else{
		return 1;
	}

}
#------------------------------------------------------------------ 
#
#------------------------------------------------------------------
sub isMatchStr(){
	my($parse_line, $sr_cnt, $diff_line)	= @_;
	my ($bcnt, $ecnt);
	($diff_line, $bcnt, $ecnt)	= @$diff_line;
	my $ret				= &isfullstr($parse_line, $diff_line);
	if ($ret == 0){
	if($bcnt != 0 or $ecnt != 0){
		if( $sr_cnt >= $bcnt and $sr_cnt <= $ecnt){
				return 0;
		}
		else{
				return 1;
			}
		}
		else{
			return 0;
		}
	}
	return 1;
}
#------------------------------------------------------------------ 
#
#------------------------------------------------------------------
sub issubstr(){
	my($full, $str)	= @_;
	my $pos		= index($full, $str);
	if( defined $pos and $pos >= 0){
		return 0;
	}
	return 1;
}

#------------------------------------------------------------------
#
#------------------------------------------------------------------
sub isfullstr(){
	my($full, $str)	= @_;
	if ($full eq $str){
		return 0;
	}
	return 1;
}

#------------------------------------------------------------------
#generate the gcov file
#------------------------------------------------------------------
sub gengcov(){
	my ($file, $file_gcno)	=@_;

	if(!defined $file_gcno){
		return 1;
	}	
	my $ret		= system("gcov $file -o $file_gcno >/dev/null  ");
        &debug_record("system gcov: gcov $file -o $file_gcno >/dev/null");
	$ret	= $ret>>8;
	if($ret == 0){
		return 0;
	}
	else{
		return 1;
	}
}

#-------------------------------------------------------
#replace the file type 
#-------------------------------------------------------
sub replaceFileType(){
	my ($str,  $target)	= @_;
	my $newstr		= $str;
	if($str	=~ /(.*)\..*?$/){
		return "$1.$target";
	}
	return undef;
}

#-------------------------------------------------------
#truncate the backspace on the head and tail
#-------------------------------------------------------
sub delbackspace(){
	my($str)	=@_;
	if( !defined $str or $str eq ""){
		return undef;
	}
	$str 	=~s/^\s+//g;
	$str 	=~s/\$\s+//g;
	if ($str eq ""){
		return undef;
	}
	return $str;
}

#------------------------------------------------------
#judge the str is in array
#------------------------------------------------------
sub isStrInArray(){
	my (@array, $line)	=@_;
	foreach my $tmp(@array){
		if($line eq $tmp){
			return 0;	
		}
	}
	return 1;
}
#-------------------------------------------------------
#justify is success to grep the line according to regex
#-------------------------------------------------------
sub isgrep(){
	my ($line, $reg)	=@_;
	if($line =~ /$reg/){
		return 0;
	}
	else{
		return 1;
	}
}


#------------------------------------------------------
#grep the result
#------------------------------------------------------
sub ppgrep(){
	my($line, $reg, $cnt)	= @_;
	my @ret			= ();
	if($line =~ /$reg/i){
		if ($cnt >=1){
			push(@ret, $1);
		}
		if ($cnt >=2){
			push(@ret, $2);
		}
		if ($cnt >=3){
			push(@ret, $3);
		}
		if ($cnt >=4){
			return undef;
		}
		return \@ret;
	}
	else{
		return undef;
	}
}

#----------------------------------------------------------
#filter the lines according to reg array
#----------------------------------------------------------
sub filterLine(){
	my($ref_reg, $line)	= @_;
	my @arr_filter		= @$ref_reg;
	foreach my $reg(@arr_filter){
		my $ret		=&isgrep($line,$reg);
		if( $ret == 0){
			return 0;
		}
	}
	return 1;
}
#------------------------------------------
#record the information
#------------------------------------------
 sub record(){
    my($string)         = @_;
    my $current_time        = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
    print("$current_time: info: $string\n");
 }

#------------------------------------------
#record the crit
#------------------------------------------
 sub crit_record(){
    my($string)         = @_;
    my $current_time        = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
    print("$current_time: crit: $string\n");
    exit(1);
 }

#------------------------------------------
#record the crit
#------------------------------------------
 sub debug_record(){
    my($string)         = @_;
    if( $debug ==1){
    	my $current_time	= strftime("%Y-%m-%d %H:%M:%S", localtime(time));
    	print("$current_time: debug: $string\n");
    }
 }
#------------------------------------------
#record the warn 
#------------------------------------------
 sub warn_record(){
    my($string)         = @_;
    my $current_time        = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
    print("$current_time: warn: $string\n");
 }


sub sp_warn_record(){
   my($str1, $str2)         = @_;
   print("\t\t\t===================================================================\n");
   print("\t\t\t=	$str1\n");
   print("\t\t\t=	$str2\n");
   print("\t\t\t===================================================================\n");
}
#---------------------------------------------------------------------
#init the file handle
#---------------------------------------------------------------------
sub getFileHandle{
    my($file, $mode)=@_;
    my ($hd);
    $hd = new FileHandle($file,"$mode");
    return $hd;
 }


#---------------------------------------------------------------------------------
#split the input parameters
#---------------------------------------------------------------------------------
sub do_opt{
    use vars '$opt_h','$opt_H','$opt_f','$opt_d','$opt_l';
    my $returncode = getopts('hH:f:d:l:');
    &do_help() if(defined $opt_h or defined $opt_H or $returncode!=1);
    $diff          = $opt_f if(defined $opt_f);
    $directory     = $opt_d if(defined $opt_d);
    $log     	   = $opt_l if(defined $opt_l);
 }
#----------------------------------------------------------------------------------
#give help information and exit;
#---------------------------------------------------------------------------------
sub do_help{
    $~ = "HEADER";
    write;
    exit(1);
 }
