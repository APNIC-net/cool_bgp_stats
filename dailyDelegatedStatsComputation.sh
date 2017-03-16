#!/bin/bash
# This script receives the path to the python script that computes statistics about delegations as first argument
# and the path to the folder where already computed statistics are stored.
# This scrips lists the provided folder looking for a csv file with stats about delegations for the current year
# If there is no file for the current year, all the stats for the current year are computed.
# If there is a file with statistics for the current year, statistics for the rest of the year (up until today)
# are computed and appended to the existing file.

pythonScript=$1
delStats_folder=$2

curr_year=$(date +'%Y')
statsForCurrYear=`ls $delStats_folder | grep "_$curr_year" | grep csv`
numOfStatsFiles=${#statsForCurrYear[@]}
case $numOfStatsFiles in
	0) echo "No file with stats about delegations for current year found."
	echo "Computing stats for the whole year"
	python $pythonScript -p $delStats_folder -e -D $curr_year
	;;
	1) echo "File with stats about delegations for current year found."
	echo $delStats_folder/$statsForCurrYear
	echo "Computing stats for the rest of the year."
	python $pythonScript -p $delStats_folder -e -i $delStats_folder/$statsForCurrYear -D $curr_year
	;;
	*) echo "More than one file with stats about delegations for current year found!"
	exit
	;;
esac
