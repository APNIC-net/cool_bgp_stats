#!/usr/local/bin/bash

# This script receives the path to the python script that computes statistics about usage of delegated resources as first argument,
# the path to the folder in which the files with computed stats will be saved as second argument,
# the path to the archive folder with historical routing data as third argument,
# the extension to look for in the archive as fourth argument and
# the hostname of a host running Elasticsearch as fifth argument.

# This script runs the python script provided using the following options:
# -f <stats folder>
# -H <archive folder>
# -e <extension>
# -N (Not readable)
# -x (eXtended)
# -D <Elasticsearch DB host>


pythonScript=$1
stats_folder=$2
archive_folder=$3
extension=$4
es_host=$5

yesterday=$(date +%Y%m%d -d "yesterday")
python $pythonScript -f /home/sofia/BGP_stats_files -H $archive_folder -e $extension -S $yesterday -E $yesterday -n
python $pythonScript -f $stats_folder -H $archive_folder -e $extension -N -T -x -D $es_host
