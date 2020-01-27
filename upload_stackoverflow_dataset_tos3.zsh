#!/usr/bin/env zsh

# This script takes 2 arguments that represent the range
# of years to upload to s3 (low_year/high_year).

low_year=$1
high_year=$2

stack_exchange_url=https://files.pushshift.io/stackexchange
stx=STX_
download_dir=/mnt
aws_bucket=s3://stackoverflow-ds

typeset -Z 2 month # zero padding month strings.
for year in {$low_year..$high_year};
do
	for month_unpadded in {1..12}; do
		month=$month_unpadded  # 1->01, 2->02,..., 10->10,..
		name="$stx$year-$month"
		name_zst="$stx$year-$month.zst"
		http_url="$stack_exchange_url/$name_zst"
		local_path_zst="$download_dir/$name_zst"
		local_path="$download_dir/$name"
		aws_url="$aws_bucket/$name"
		aws_ls=`aws s3 ls $aws_url`
		if [[ ! -n $aws_ls ]];
		then
			cmd="wget -P $download_dir $http_url && unzstd --rm $local_path_zst -o $local_path && aws s3 cp $local_path $aws_bucket && rm -f $local_url"
			echo "Executing: $cmd"
			eval {$cmd}
		else
			echo "Skip downloading $http_url since $aws_url exists."
		fi
	done
done
