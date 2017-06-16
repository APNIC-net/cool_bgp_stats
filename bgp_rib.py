"""
@author Alejandro Acosta
Minor fixes by Sofia Silva Berenguer
"""

import sys
import datetime
from collections import namedtuple
import re
import dateutil.parser
#from  netaddr import IPAddress, IPNetwork

def get_class_length(prefix):

    digits = prefix.split('.')

    try:
        lead = int(digits[0])
    except ValueError:
        return -1

    if lead < 128:
        return 8

    if lead <= 191:
        return 16

    if lead <= 239:
        return 24


def namedtupledict(*a, **kw):
    """
    A namedtuple that can also be accesed using the brakets. I got the code
    from the Internet but I changed some details.
    namedtuple were introduced in python 2.6
    TODO override other functions in namedtupledict:
    To make this really a dict, I should also override:
        *__contains__() : to make sure that the element is checked against
        the properties and not the values.
    """
    namedtuple_inst = namedtuple(*a, **kw)

    def getitem(self, key):
        if isinstance(key, str):
            return getattr(self, key)
        return tuple.__getitem__(self, key)
    namedtuple_inst.__getitem__ = getitem
    return namedtuple_inst


class ASN(int):
    """
    An AS number could behave like a general string, but could contain
    future functions accounting for other characteristics:
        * Validate if the AS number is valid.
        * Return Private / Public or 16 / 32 bits depending on the type.
        * Handle confederations and other cases.
        * Handy functions for later filtering.
        * Etc.
    TODO
    Store the numbers in numbers and override the required functions to make
    the class work aas a strng when necessary.
    TODO:
        This takes memory, check if using c or cdef
        classes can improve the memory footprint
    """
    def __new__(cls, asnumber):
        try:
            if isinstance(asnumber, basestring):
                if asnumber.find(".") > -1:
                    prenumber = int(asnumber[:asnumber.find(".")])
                    postnumber = int(asnumber[asnumber.find(".") + 1:])
                    newasn = int.__new__(cls, prenumber * 65536 + postnumber)
                else:
                    newasn = int.__new__(cls, asnumber)
            else:
                newasn = int.__new__(cls, asnumber)
        except:
            raise NameError("ASN %s format not recognized" % (asnumber))

        return newasn


class ASPath(tuple):
    """
    A typical list that should only be used with ASN numbers.
    Future functions of this class can contain:
        * Filtering of ASN.
        * Giving errors or warnings if there are
             non-continued repeated values of ASN.
        * Giving errors or warnings if ASN are not valid.
        * Metrics.
        * Removing appending
        * Comparison with other ASPath (Edit Distance, etc.)
    TODO
    Write some functiosn to make sure that this can read different
    strings of as_paths.
    """
    def __new__(cls, asn_tuple):
        new_asn_list = []
         #TODO handle summarization {}
        for asn in asn_tuple:
            if isinstance(asn, str):
                if asn.find('{') > -1 or asn.find('}') > -1:
                    asn = asn.strip('{').strip('}')
            try:
                new_asn_list.append(int(ASN(asn)))
            except:
                print 'Problem with AS_PATH %s' % (asn_tuple)
                raise
        return tuple.__new__(cls, new_asn_list)

    def filter(self, i_filter=True, qm_filter=True, e_filter=True):
        """
        Returns the AS PATH after filtering some common problems.
        """
        new_as_path = []
        for asn in self:
            if i_filter and asn == 'i':
                continue
            if qm_filter and asn == '?':
                continue
            if e_filter and asn == 'e':
                continue
            new_as_path.append(asn)
        return ASPath(new_as_path)

    def check_loops(self, report=True):
        as_set = set()
        repeated_as = set()
        for asn in self:
            if asn in as_set:
                repeated_as.add(asn)
            else:
                as_set.add(asn)

        if report:
            sys.stderr.write('Repeated ASN were found: %s' % (repeated_as))
        return repeated_as


class BGPPrefixInfo(tuple):
    """
    A BGPPrefixInfo hols the required information of a prefix.
    The implementation is done via a tuple of 2-tuples (not tested),
    which can be easily converted to dicts for easily indexing.
    In the future, this class could contain functions that can help:
        * Compare Prefix paths.
    TODO have just one big database of attributes to save memory.
    """
    def characteristics(self, value=0):
        """
        Returns the first values of each tuple, which could be considered as
        a column name in a DB.
        The name keys on this function will break the python code (no idea
        why just having a function named keys breaks something, but anyway).
        Numberic Characteristics will not work. The class does not check
        this.
        """
        this_keys = []
        for characteristic in self:
            try:
                this_keys.append(characteristic[value])
            except:
                raise NameError('tuple %s does not have an element \
                        in the value' % (characteristic))

        ## Another way, but probably this one is less efficient.
        ## We can consider using a generator if keys are actually only used in
        ## iterators.
        #try:
        #    this_dict = dict(self)
        #except:
        #    raise NameError('Data is not propertly stored')

        #return this_dict.keys()

        return this_keys

    def __getitem__(self, index):
        if isinstance(index, str):
            try:
                this_dict = dict(self)
            except:
                raise NameError('Data cannot be converted to dict')

            try:
                return this_dict[index]
            except:
                raise NameError('Characteritic %s is not included' % (index))

        else:
            return tuple.__getitem__(self, index)


class SetPrefixInfo(set):
    """
    Holds a set of BGPInfo and provides some function to ease the
    accesibility of the information.
    """
    def __getitem__(self, index):
        """
        For a specific characteristic (index) returns a dict:
            keys are each possible value of the characteristic in
            the present data.
            Values are a list of BGPInfo which value of characteristic
            is the key.
        """
        resulting_dict = {}
        for bgp_info in self:
            char_value = bgp_info[index]
            if char_value not in resulting_dict:
                resulting_dict[char_value] = SetPrefixInfo()
            resulting_dict[char_value].add(bgp_info)
        return resulting_dict


class BGPRIB(dict):
    """
    A GenericRIB behaves as a dict, but it should only contain
    IP networks as keys and values should be sets (this is actually not
    tested).
    """
    @classmethod
    def merge(cls, rib1, rib2):
        """
        Returns a RIB obtained by merging the two given RIBs.
        """
        merged_rib = BGPRIB(rib1)
        for prefix in rib2:
            if prefix not in merged_rib:
                merged_rib[prefix] = SetPrefixInfo()
            merged_rib[prefix] = merged_rib[prefix].union(rib2[prefix])
        return merged_rib

    @classmethod
    def merge_rib_list(cls, list_of_rib):
        """
        Returns the RIB obtained after merging all ribs
        from the given list.
        """
        merged_rib = None
        for counter, rib in enumerate(list_of_rib):
            print "%d of %d" %(counter, len(list_of_rib))
            if counter == 0:
                merged_rib = rib
            else:
                merged_rib = cls.merge(merged_rib, rib)
        return merged_rib

    @classmethod
    def difference(cls, rib1, rib2):
        """
        Returns a dict structure (diff) that holds (careful, the order of ribs
        is important):
            * diff[prefix] = None if rib2 does not possess any info for the
            prefix.
            * diff[prefix] = list_of_info for the info on rib1 that is not
            in rib2
            if there is rib2 has all the info from rib1, the prefix is not
                included
        """
        diff = {}
        for prefix in rib1:
            if prefix not in rib2:
                diff[prefix] = None
            else:
                difference = rib1[prefix] - rib2[prefix]
                if difference:
                    diff[prefix] = difference

        return diff


    @classmethod
    def parse_cisco_show_ip_bgp_offsets_generator(cls, file_h):
        """
        Returns lines of a show ip bgg output from cisco while also detecting the offst.
        This code does not detect illegal lines yet TODO
        """
        network = ""
        previous_network = ""
        double_line = False

        for linecpt, line in enumerate(file_h):
            try:

                if linecpt == 0:
                    if line[0] == ' ':
                        offset_1 = 1
                        offset_2 = 2
                        offset_3 = 1
                    else:
                        offset_1 = 0
                        offset_2 = 0
                        offset_3 = 0

                linecpt = linecpt + 1
                line = line.rstrip()
                offset_dl = 0
                if not double_line:
                    if len(line) < 62:
                        #print "#DEBUG Double line entry:"
                        #print current_line
                        network = line[3 + offset_2 :len(line)].rstrip()
                        bgp_type = line[2 + offset_1]
                        double_line = True
                        continue
                    else:
                        network = line[3 + offset_2 : 20 + offset_2].rstrip()
                        if network == "":
                            network = previous_network
                        bgp_type = line[2 + offset_1]
                else:
                    offset_dl = offset_3
                    double_line = False

                previous_network = network


                if '/' not in network:
                    #print "#DEBUG no prefix length : " + pfx
                    pfx = get_class_length(network)
                    if pfx != -1:
                        network = network + "/" + str(pfx)
                    else:
                        continue
                #print "#DEBUG Prefix: " + pfx


                nexthop = line[20 + offset_2 + offset_dl: 36 + offset_2 + offset_dl].rstrip()
                #print "#DEBUG NH : " + nexthop

                metric = line[37 + offset_2 + offset_dl : 47 + offset_2 + offset_dl].strip()
                #print "#DEBUG METRIC : " + metric

                local_pref = line[48 + offset_2 + offset_dl: 54 + offset_2 + offset_dl].strip()
                #print "#DEBUG LOC_PREF : " + local_pref

                weight = line[55 + offset_2 + offset_dl: 60 + offset_2 + offset_dl].strip()
                #print "#DEBUG WEIGHT : " + weight

                as_path = line[61 + offset_2 + offset_dl:].rstrip()
                as_path = as_path[0:len(as_path) - 2]
                as_path = as_path.split(' ')
                if as_path == ['']:
                    as_path = []

                #print "#DEBUG AS_PATH : " + as_path

                origin = line[len(line) - 1]


            except:
                print "Error at line " + str(linecpt) + str(line)
                raise

            yield (network, bgp_type, nexthop, metric, local_pref, weight, as_path, origin)





    @classmethod
    def parse_cisco_show_ip_bgp_generator(cls, file_name):

        #show_bgp_file = open(filename)

        network = ""
        previous_network = ""
        nexthop = ""
        multiple_lines = False
        start_process = False

        dates = re.findall('''(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])''', file_name)
       
        if len(dates) > 0:
            year = int(dates[0][0])
            month = int(dates[0][1])
            day = int(dates[0][2])
            date = datetime.date(year, month, day).strftime('%Y%m%d%H%M')
        else:
            # If there is no date in the file name, we use the date of today
            date = datetime.datetime.today().strftime('%Y%m%d%H%M')

        with open(file_name, 'r') as file_h:
            for linecpt, line in enumerate(file_h):
                try:
                    # We check whether there is a date in the first line
                    # If there is, we use that date
                    if linecpt == 0:
                        try:
                            date = dateutil.parser.parse(line.strip()).date().strftime('%Y%m%d%H%M')
                        except ValueError:
                            pass
                    # start in the point where the first valid line is found (*)
                    if not line.strip():
                        continue
                    if line.startswith("Total") or line.startswith('Displayed'):
                        # I found these lines at the end
                        continue
                    if line[0:6] == "#DATE:":
                        date = line[7:19]
                        continue
                    if line and not start_process:
                        if line[0] == "*" or line[1] == '*':
                            start_process = True
                        else:
                            continue

                    #if linecpt % 400000 == 0:
                        #print linecpt
                    linecpt = linecpt + 1
                    line = line.rstrip()
                    line_parts = line.split()
                    if not multiple_lines:
                        if line[2:20].strip() == '':
                            network = previous_network
                            nexthop = line_parts[1]
                        else:
                            if len(line_parts) > 1 and len(line_parts[1]) > 18:
                                field_parts = line_parts[1].split('/')
                                network = '{}/{}'.format(field_parts[0], field_parts[1][0:2])
                                nexthop = field_parts[1][2:]
                            else:
                                if len(line_parts) > 1:
                                    network = line_parts[1]
                                if len(line_parts) > 2:
                                    nexthop = line_parts[2]

                        bgp_type = line[2]

                        if len(line) < 62:
                            multiple_lines = True
                            continue
                        else:
                            if network == "":
                                network = previous_network

                    previous_network = network

                    if '/' not in network:
                        #print "#DEBUG no prefix length : " + pfx
                        pfx = get_class_length(network)
                        if pfx != -1:
                            network = network + "/" + str(pfx)
                        else:
                            continue
                    #print "#DEBUG Prefix: " + pfx

                    if nexthop == "":
                        nexthop = line_parts[0]
                        #print "#DEBUG NH : " + nexthop
                        if len(line_parts) == 1:
                            continue

                    metric = line[37:47].strip()
                    #print "#DEBUG METRIC : " + metric

                    local_pref = line[48:54].strip()
                    #print "#DEBUG LOC_PREF : " + local_pref

                    weight = line[55:60].strip()
                    #print "#DEBUG WEIGHT : " + weight

                    as_path = line[61:].rstrip()
                    as_path = as_path[0:len(as_path) - 2]
                    as_path = as_path.split(' ')
                    if as_path == ['']:
                        as_path = []
                    as_path = tuple(as_path)

                    #print "#DEBUG AS_PATH : " + as_path

                    origin = line[len(line) - 1]

                    multiple_lines = False
                    nexthop_for_yield = nexthop
                    nexthop = ""

                except:
                    print "Error at line " + str(linecpt) + str(line)
                    raise

                yield (network, bgp_type, nexthop_for_yield, metric, local_pref,\
                        weight, as_path, origin, date)

            file_h.close()


    @classmethod
    def parse_cisco_show_ip_bgp(cls, filename, as_number=None, \
            inc_nexthop=False, inc_aspath=True, inc_locpref=False, \
            inc_weigth=False, inc_metric=False, inc_line=False, \
            inc_originator=False, inc_bgp_type=False, inc_path_length=False,\
            inc_real_nh=False, inc_first_as=0, next_hop_self=None):
        """
        Parses a show ip bgp and returns a BGPRIB object with the info.
        This function uses the prefix as key for the BGPRIP.
        TODO
        Some of the values get a default if they not appear.
        The default for MED is 0, which is fine, the default for LP is 100, right
        not the function is setting 0, dont know if I should fix it.
        """

        show_bgp_file = open(filename)

        network = ""
        previous_network = ""
        double_line = False

        # Create a namedtupledict that fits the requirements of the user.
        # A namedtupledict is a tuple that can be also referenceded by a
        # string. This kind of solves the problem if not having unmutable
        # dicts and is more general than using a personalized function.

        # Check which properties must be included.
        properties = []
        if inc_nexthop:
            properties.append('next_hop')
        if inc_bgp_type:
            properties.append('ibgp')
        if inc_aspath:
            properties.append('as_path')
        if inc_locpref:
            properties.append('locpref')
        if inc_weigth:
            properties.append('weigth')
        if inc_metric:
            properties.append('metric')
        if inc_line:
            properties.append('line')
        if inc_originator:
            properties.append('originator')
        if inc_path_length:
            properties.append('path_length')
        if inc_first_as > 0:
            properties.append('partial_as_path')
        if inc_real_nh:
            properties.append('real_nh')

        PrefixInfo = namedtupledict('PrefixInfo', properties)

        #PrefixInfo = namedtuple('PrefixInfo', 'next_hop rest')
        rib_in = BGPRIB()
        try:
            for linecpt, line in enumerate(show_bgp_file):
                if linecpt == 0:
                    if line[0] == ' ':
                        offset_1 = 1
                        offset_2 = 2
                        offset_3 = 1
                    else:
                        offset_1 = 0
                        offset_2 = 0
                        offset_3 = 0

                if linecpt % 50000 == 0:
                    print linecpt
                linecpt = linecpt + 1
                line = line.rstrip()
                offset_dl = 0
                if not double_line:
                    if line[3] not in range(0, 10):
                        # Not a valid line
                        continue
                    if len(line) < 62:
                        #print "#DEBUG Double line entry:"
                        #print current_line
                        network = line[3 + offset_2 :len(line)].rstrip()
                        bgp_type = line[2 + offset_1]
                        double_line = True
                        continue
                    else:
                        network = line[3 + offset_2 : 20 + offset_2].rstrip()
                        if network == "":
                            network = previous_network
                        bgp_type = line[2 + offset_1]
                else:
                    offset_dl = offset_3
                    double_line = False

                previous_network = network


                if '/' not in network:
                    #print "#DEBUG no prefix length : " + pfx
                    pfx = get_class_length(network)
                    if pfx != -1:
                        network = network + "/" + str(pfx)
                    else:
                        continue
                #print "#DEBUG Prefix: " + pfx


                nexthop = line[20 + offset_2 + offset_dl: 36 + offset_2 + offset_dl].rstrip()
                #print "#DEBUG NH : " + nexthop

                metric = line[37 + offset_2 + offset_dl : 47 + offset_2 + offset_dl].strip()
                #print "#DEBUG METRIC : " + metric

                local_pref = line[48 + offset_2 + offset_dl: 54 + offset_2 + offset_dl].strip()
                #print "#DEBUG LOC_PREF : " + local_pref

                weight = line[55 + offset_2 + offset_dl: 60 + offset_2 + offset_dl].strip()
                #print "#DEBUG WEIGHT : " + weight

                as_path = line[61 + offset_2 + offset_dl:].rstrip()
                as_path = as_path[0:len(as_path) - 2]
                as_path = as_path.split(' ')
                if as_path == ['']:
                    as_path = []

                #print "#DEBUG AS_PATH : " + as_path

                origin = line[len(line) - 1]


                if network not in rib_in:
                    rib_in[network] = SetPrefixInfo()

                properties = {}
                if inc_real_nh:
                    properties['real_nh'] = nexthop
                if inc_nexthop:
                    if next_hop_self and not bgp_type == 'i':
                        properties['next_hop'] = next_hop_self
                    else:
                        properties['next_hop'] = nexthop
                    #properties['next_hop'] = \
                    #        int(IPAddress((nexthop)))
                if inc_bgp_type:
                    if bgp_type == 'i':
                        properties['ibgp'] = True
                    elif bgp_type == ' ':
                        properties['ibgp'] = False
                    else:
                        print "#WARNING LINE %d, BGP type unknown %s" %(linecpt, bgp_type)
                if inc_locpref:
                    if local_pref == '':
                        # TODO, SET TO 100 IF NOT THERE????
                        properties['locpref'] = 0
                    else:
                        properties['locpref'] = int(local_pref)
                if inc_weigth:
                    if weight == '':
                        properties['weigth'] = 0
                    else:
                        properties['weigth'] = int(weight)

                if inc_metric:
                    if metric == '':
                        properties['metric'] = 0
                    else:
                        properties['metric'] = int(metric)
                if inc_line:
                    #properties['line'] = line
                    properties['line'] = line[20:len(line)]
                if inc_originator:
                    properties['originator'] = origin
                if inc_aspath:
                    # prepare the as_path
                    if as_number:
                        temp_as_path = as_path
                        temp_as_path.insert(0, as_number)
                        aspath = ASPath(temp_as_path)
                    else:
                        aspath = ASPath(as_path)
                    # Here we could filter the as path.
                    # This is slow (cProfile),
                    # thus we are just going to not do anymore.
                    #aspath = aspath.filter(i_filter=True)
                    properties['as_path'] = aspath

                if inc_path_length:
                    properties['path_length'] = len(as_path)

                if inc_first_as > 0:
                    properties['partial_as_path'] = ASPath(as_path[0:inc_first_as])

                #prefix_info = properties['next_hop']
                prefix_info = PrefixInfo(**properties)
                rib_in[network].add(prefix_info)

        except:
            print "Error at line " + str(linecpt)
            raise

        finally:
            show_bgp_file.close()

        return rib_in

    # TODO WRITE AND READ FROM CSV
    # TODO Do a function that yields the values

    def create_dict(self, key, first_hierarchy=False):
        """
        Creates a dictionary with the information of the BGPRIB.
        If the key is set as first_hierarchy, the second_hierarchy
        will be the one used currently by the BGPRIB.
        """
        resulting_dict = {}

        if not first_hierarchy:
            for prefix, prefix_routes in self.items():
                if prefix not in resulting_dict:
                    resulting_dict[prefix] = {}
                for prefix_info in prefix_routes:
                    second_key = prefix_info[key]
                    if second_key not in resulting_dict[prefix]:
                        resulting_dict[prefix][second_key] = SetPrefixInfo()
                    resulting_dict[prefix][second_key].add(prefix_info)

        else:
            for prefix, prefix_routes in self.items():
                for prefix_info in prefix_routes:
                    primary_key = prefix_info[key]
                    if primary_key not in resulting_dict:
                        resulting_dict[primary_key] = {}

                    if prefix not in resulting_dict[primary_key]:
                        resulting_dict[primary_key][prefix] = SetPrefixInfo()

                    resulting_dict[primary_key][prefix].add(prefix_info)

        return resulting_dict

    @classmethod
    def read_from_plain_file(cls, filename, separator='|', \
            key_char='prefix', \
            characteristic_list=('prefix', 'as_path')):
        """
        Reads from a plain text file the characteristics. If no key
        characteristic is defined, the first will be used.
        The order of the characteristic_list is important.
        example lists.
        characteristic_list=(None, None, None, 'fromip', 'fromas', \
        'prefix', 'as_path', 'origin', 'next_hop', None, None, None, \
         None)
         characteristic_list=(None, None, None, None, None, \$
         414             'prefix', 'as_path', None)):
        """
        # Test that the input is correct.
        if isinstance(key_char, str):
            if key_char not in characteristic_list:
                raise NameError("key characteristic is not in the list")
            else:
                key_char = characteristic_list.index(key_char)
        elif not isinstance(key_char, int):
            raise NameError("Type of key_char not supported. Int and Str only")

        # Convert the characteristic into a dict(position) -> characteristic
        char_position = {}
        for position, characteristic in enumerate(characteristic_list):
            if characteristic is not None:
                char_position[position] = characteristic

        # TODO Change code to be consisitent on key value. Either everything
        # should be based on prefix as the key of the DB (and add code to not
        # mix IPv4 with IPv6) or change everythign that says prefix to simple
        # key_value.
        PrefixInfo = namedtupledict('PrefixInfo', [char_position[prop] for prop\
                                    in char_position if not prop == key_char])
        filehandler = open(filename, 'r')

        rib_in = BGPRIB()

        #try:
        for linecpt, line in enumerate(filehandler):
            if linecpt % 100000 == 0:
                print linecpt

            # TODO Make this code more robust to failures
            # TODO remove the need of having the key in the prefie
            lineinfo = line.split(separator)
            try:
                properties = dict([(char_position[position], lineinfo[position]) \
                        for position in char_position if not position == key_char])
                        #for position in char_position])
                        # for position in char_position if not position == key_char])
            except:
                print line, lineinfo
                raise NameError("Problem parsing file. Are you sure that the \
                        characteristic list fits the file?")
            # TODO prefix should be changed here for key_valye. Actually try to
            # be consistent and differenciate between NLRI, Prefix and Network..
            # Actually try to be consistent and differenciate between NLRI,
            # Prefix and Network.
            prefix = lineinfo[key_char]

            if prefix not in rib_in:
                rib_in[prefix] = SetPrefixInfo()

            prefix_info = PrefixInfo(**properties)
            #prefix_info = tuple(properties.values())
            #print prefix_info
            rib_in[prefix].add(prefix_info)

        #finally:
        filehandler.close()
        return rib_in

    def write_to_mysql(self, database, table_name, \
            prefix_col, characteristic_mapping=None):
        """
        Write the RIB in a mysql database.
        """

        # Create the insert string
        common_string = "INSERT INTO %s " % (table_name)

        for prefix, prefix_data in self.items():
            for prefix_info in prefix_data:
                command = []
                command.append(common_string)

                fields = prefix_info._fields

                col_string = [prefix_col]
                values_string = ['%s,']
                values = [prefix]

                for field in fields:
                    if field in characteristic_mapping:
                        col_string.append(characteristic_mapping[field])

                        values.append("%s")
                        values_string.append(str(prefix_info[field]))

            command.append("(")
            command.append(','.join(col_string))
            command.append(") VALUES(")
            command.append(','.join(values_string))

            try:
                with database:
                    cur = database.cursor()
                    cur.execute(command, tuple(values_string))
            except:
                raise NameError('Error inserting value %s for \
                        prefix %s' % (str(prefix_data), str(prefix)))

    def write_to_file(self, filename, separator=',', characteristic_list=None):
        """
        Write the RIB in a text file.
        """
        try:
            filehandler = open(filename, 'w')
        except IOError:
            print 'File cannot be open'
            raise

        if characteristic_list is None:
            # In order to make the object simpler, I do not follow
            # the number of characteristics that are saved for each
            # prefix, therefore, I do two loops to write the data:
            # first loop checks for all posible characteristics
            #     in the database.
            # Second loop creates the file.
            characteristic_list = set()
            for prefix_data in self.values():
                for prefix_info in prefix_data:
                    characteristic_list.update(prefix_info._fields)

            characteristic_list = list(characteristic_list)

#        filehandler.write(separator.join(characteristic_list))

        for prefix, prefix_data in self.items():
            for prefix_info in prefix_data:
                character_set = set(prefix_info._fields)
                line = []
                line.append(prefix)
                for characteristic in characteristic_list:
                    if characteristic in character_set:
                        # TODO
                        # obtain the info. Make this better using
                        # __contain__ (in) when I write it.
                        try:
                            info = prefix_info[characteristic]
                        except:
                            info = ''
                        line.append(str(info))
                filehandler.write(separator.join(line))
                filehandler.write('\n')
        filehandler.close()

    def create_network(self, as_path_attr_name='as_path', \
            parse_as=False):
        """
        Returns a undirected network using all as_paths found
        in the information on each prefix. Prefixes that do
        not have the as-path attribute are ignored.
        """
        import networkx as nx
        new_graph = nx.Graph()

        for prefix, prefix_data in self.items():
            for prefix_info in prefix_data:
                if as_path_attr_name in prefix_info._fields:
                    as_path = prefix_info[as_path_attr_name]
                else:
                    continue

                if parse_as:
                    try:
                        if as_path == '':
                            as_path = ()
                        else:
                            as_path_temp = ASPath(as_path.split(' '))
                            as_path = as_path_temp
                    except:
                        print 'Invalid AS_Path: %s for prefix %s' %(as_path, prefix)
                        as_path = ()
                # Create an edge for the graph between each one of the adjacent
                # pairs found in the as_path
                for as_pair in \
                        ((as_path[i], as_path[i + 1]) for i in \
                        xrange(0, len(as_path) - 1)):
                    new_graph.add_edge(as_pair[0], as_pair[1])

        return new_graph


class DictTuple(tuple):
    """
    A tuple that one can also be indexed using words (it must be set up first).
    Check, probably this is the one with problems
    """

    def __new__(cls, *args):
        cls.index_dict = {}
        return tuple.__new__(cls, *args)

    def set_words(self, index_dict):
        """
        Sets the index_dict. We never test for the correctness
        of the values of the dict.
        """
        self.index_dict = index_dict

    def __getitem__(self, index):
        if index in self.index_dict:
            index = self.index_dict[index]
            try:
                return tuple.__getitem__(self, index)
            except:
                sys.stderr.write('index taken from the given index_dict')
                raise
        else:
            return tuple.__getitem__(self, index)
