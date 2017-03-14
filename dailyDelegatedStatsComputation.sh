#!/bin/bash

pythonScript="/Users/sofiasilva/GitHub/cool_bgp_stats/delegated_stats_v5.py"

delStats_folder="/Users/sofiasilva/BGP_files/DelegatedStats"
curr_year=$(date +'%Y')
statsForCurrYear=`ls $delStats_folder | grep "_$curr_year\_" | grep csv`
numOfStatsFiles=${#statsForCurrYear[@]}
case $numOfStatsFiles in
	0) echo "No file with stats about delegations for current year found."
	echo "Computing stats for the whole year"
	python $pythonScript -p $delStats_folder -e -y $curr_year
	;;
	1) echo "File with stats about delegations for current year found."
	echo $delStats_folder/$statsForCurrYear
	echo "Computing stats for the rest of the year."
	python $pythonScript -p $delStats_folder -e -i $delStats_folder/$statsForCurrYear -y $curr_year
	;;
	*) echo "More than one file with stats about delegations for current year found!"
	exit
	;;
esac
