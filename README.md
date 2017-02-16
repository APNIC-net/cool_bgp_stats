# cool_bgp_stats

* cool_bgp_stats_Django folder
Folder containing Django app to make the computed stats public (Under development)

* BGPDataHandler.py
Script that defines the BGPDataHandler class which contains all the functions used to download and process BGP data.
An object of class BGPDataHandler contains:
	- bgp_data -> a DataFrame with the BGP information extracted from RIB files or from files containing 'show ip bgp' output
	- prefixes_indexes_pyt -> a PyTricia containing announced prefixes as keys and as values lists of indexes of the rows in the bgp_data DataFrame with BGP announcements related to the corresponding prefix.
	- ASes_prefixes_dic -> a dictionary containing ASes as keys and as values lists of the prefixes announced by the corresponding AS.

* BGPoutputs.txt
Text file containing a list of URLs pointing to files containing 'show ip bgp' outputs. (By now the file contains a single URL)

* Collections.txt
Text file containing information about regions, orgamizations, etc. related to each economy. (This file is used to determine the region for an economy.)

* Collectors.txt
Text file containing a list of URLs pointing to RIB files. (By now the file contains a single URL)

* DelegatedHandler.py
Script that defines the DelegatedHandler class which includes all the variables and methods related to delegated file handling.
An object of class DelegatedHandler contains (among other informative variables):
	- delegated_df -> a DataFrame with information extracted from a delegated file.
 
* bgp_rib.py
Script by Alejando Acosta that parses a 'show ip bgp' output and converts it to the format used by BGPdump for its output.

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

The statistics computed are exported to a csv file and to a json file.cript that downloads content provided its URL, after checking if file has already been downloaded and if it is still fresh.

* delegated_stats_v2.py
Second version of delegated_stats.py script. Cleaner, more readable and easier to execute.
Besides, it performs much better as the only granularity considered is daily and
statistics are computed for a specific year.
Then the statistics can be aggregated in order to get statistics with other granularities.
Has to be executed separately for the different years of interest.

* delegated_stats_v3.py
Third version of delegated_stats.py script. Now this script doesn't include all the variables and methods related to delegated file handling as they were extracted to the DelegatedHandler class in a separate script. This script instantiates DelegatedHandler and then computes the statistics. The '-y' option is not mandatory anymore. If the script is executed without using the '-y' option, the statistics will be computed for all the years available in the delegated file.
Comments in the script provide further detail about how it works.

* delegated_stats_v4_DO_NOT_USE.py
This script was created with the purpose of making the computation of statistics about delegations more efficient. Instead of initializing the stats_df DataFrame with all the possible combinations of the index columns values, rows are created when necessary. I thought this would perform better, however, the fact of having to sort the index after adding a new row probably makes it perform worse. Indeed, comparing the execution of v3 of the script and v4 of the script I found out that v3 is faster.
This script is kept in the repository as a reference but it SHOULD NOT BE USED (Use delegated_stats_v3.py instead.)

* get_file.py
Script by Carlos Martinez that downloads a file provided its URL.

* plotStats.py
Basic script that reads file with statistics, filters the stats for the specific values of the different variables provided
and generates plots. (Under development)

* routing_stats.py
Script that instantiates the BGPDataHandler and the DelegatedHandler classes and computes statistics on a per prefix basis.
(Under development)
