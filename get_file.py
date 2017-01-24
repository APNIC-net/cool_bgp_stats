# carlos@lacnic.net 20121128
# version 0.1

#===============================================================================
# Copyright (c) 2012 LACNIC - Latin American and Caribbean Internet 
# Address Registry
# 
# Permission is hereby granted, free of charge, to any person 
# obtaining a copy of this software and associated documentation 
# files (the "Software"), to deal in the Software without 
# restriction, including without limitation the rights to use, copy, 
# modify, merge, publish, distribute, sublicense, and/or sell copies 
# of the Software, and to permit persons to whom the Software is 
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be 
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS 
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN 
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
# SOFTWARE.
#===============================================================================

import urllib2
import os
import sys
import gzip
#import ipaddr
import time

## get file ########################################################################
def get_file(w_url, w_file, w_update = 3600, ch_size=10*1024):
    """
    Downloads a file object pointed by w_url and stores it on local file w_file.
    The w_update parameter marks how old the file can be. Files are only downloaded 
    if they are older than w_update seconds.
    """
    try:
        sys.stderr.write("Getting "+w_url+": ")
        mtime = 0
        if os.path.exists(w_file):
            mtime = os.stat(w_file).st_mtime
        now = time.time()
        if now-mtime >= w_update:
            uh = urllib2.urlopen(w_url)
            lfh = open(w_file, "wb+")
            # lfh.write(uh.read())
            while True:
                data = uh.read(ch_size)
                if not data:
                    print ": done!"
                    break
                lfh.write(data)
                sys.stderr.write(".")
        else:
            sys.stderr.write("File exists and still fresh (%s secs old) \n" % (now-mtime) )
            return True
    except urllib2.URLError as e:
        raise
        print "URL Error", e.code, w_url
        return False
    except:
        raise
## end get file ########################################################################

if __name__ == "__main__":
    print "get_file should not be used directly"