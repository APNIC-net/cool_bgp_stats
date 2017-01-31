# cool_bgp_stats

* R script delegatedStats.R in folder Rscripts:
R script to download and process a delegated file and generate statistics.
The development of this script was suspended after deciding to implement the computation of statistics using Python and not R.

* Files in folder shiny_cool_bgp_stats:
Files for Shiny app to create interactive plots of stats.
Development of this app was suspended after deciding to implement the computation of statistics using Python and not R.

* downloadAndProcess.py
Basic script that downloads a file with BGP routing data, unzips it and decodes it using BGPDump. (Under development)

* processDump.py
Basic script that reads dump file line by line extracting peer, prefix, AS path, origin AS and origin data source. (Under development)

* get_file.py
Script that downloads content provided its URL, after checking if file has already been downloaded and if it is still fresh.

* delegated_stats.py
Initial version of script that downloads delegated or delegated extended file from APNIC's ftp server and processes it in order to generate statistics about resources delegated by APNIC.
Statistics for different combinations of the following variables are computed:
  - Granularity (All, Annually, Monthly, Weekly, Daily)
  - Resource Type (ASN, IPv4, IPv6)
  - Status (Available, Reserved, Allocated, Assigned, Alloc-32bits, Alloc-16bits)
  - Country
  - Organization (Using opaque id from delegated extended file)

The following statistics are computed:
  - NumOfDelegations: Counts the number of delegations performed by APNIC
    (Basically counts the number of rows in the delegated file)
  - NumOfResources:
      - For IPv4 and for IPv6 NumOfResources = NumOfDelegations as IP blocks delegations are shown in the delegated files as one block per line
      - For ASNs NumOfResources counts the number of ASNs assigned which is not necessarily equal to NumOfDelegations as more than one ASN may be delegated at a time
  - IPCount:
      - For IPv4 counts the number of IPs assigned/allocated
      - For IPv6 counts the number of /56 blocks assigned/allocated
      - For ASNs IPCount = -1
  - IPSpace:
      - For IPv4 contains the number of /24 blocks assigned/allocated
      - For IPv6 contains the number of /48 blocks assigned/allocated
      - For ASNs IPSpace = -1
      
The statistics computed are exported to a csv file and to a json file.

* delegated_stats_v2.py
Second version of delegated_stats.py script. Cleaner, more readable and easier to execute.
Besides, it performs much better as the only granularity considered is daily and
statistics are computed for a specific year.
Then the statistics can be aggregated in order to get statistics with other granularities.
Has to be executed separately for the different years of interest.

* plotStats.py
Basic script that reads file with statistics, filters the stats for the specific values of the different variables provided
and generates plots. (Under development)
