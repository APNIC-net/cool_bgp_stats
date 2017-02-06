# cool_bgp_stats

* R script delegatedStats.R in folder Rscripts:
R script to download and process a delegated file and generate statistics.
The development of this script was suspended after deciding to implement the computation of statistics using Python and not R.

* Files in folder shiny_cool_bgp_stats:
Files for Shiny app to create interactive plots of stats.
Development of this app was suspended after deciding to implement the computation of statistics using Python and not R.

* downloadAndProcess.py
	- Basic script that downloads a file with BGP routing data, unzips it and decodes it using BGPDump.
	- Each line of the decoded routing data is parsed in order to extract the announced prefix and the AS originating it. A dictionary is created using the announced prefixes as keys and having a list of ASes originating each prefix as data.
	- Then, the DelegatedHandler class is instantiated in order to get recent delegated info. Each line in the delegated file corresponding to IPv4 resources is converted into as many lines including CIDR blocks as necessary.
	- The resources (both IPv4 and IPv6) delegated to each organization are aggregated as much as possible.
	- Each aggregated delegated block is compared to routed blocks to determine how it is being routed.
	- Once we have all the sub-blocks being routed for each aggregated delegated block, the visibility and the deaggregation for the delegated block are computed.
	- The visibility of a blocks measures how much of a delegated block is being announced.
	- The deaggregation of a delegated block is computed as 1 minus the ratio between the number of aggregated routed blocks over the number of blocks actually being routed.
	- Finally, for each organization we generate a summary including all the blocks that were delegated to it, all the blocks being routed, the average visibility and the average deaggregation.  

(Under development)

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

* delegated_stats_v3.py
Third version of delegated_stats.py script. Now this script doesn't include all the variables and methods related to delegated file handling as they were extracted to a the DelegatedHandler class in a separate script. This script instantiates DelegatedHandler and then computes the statistics. Now the '-y' option is not mandatory anymore. If the script is executed without using the '-y' option, the statistics will be computed for all the years available in the delegated file.

* DelegatedHandler.py
Script that defined DelegatedHanlder class which includes all the variables and methods related to delegated file handling.

* plotStats.py
Basic script that reads file with statistics, filters the stats for the specific values of the different variables provided
and generates plots. (Under development)
