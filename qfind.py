#!/usr/bin/env python
import os
import sys
from qumulo.rest_client import RestClient
import arrow
import argparse
from fnmatch import translate as fnmatch
import re
import json
from datetime import datetime
from multiprocessing import Pool

############config file location
configpath = './config.json'
################################

# Import qconfig.json for login information
def getconfig(configpath):
    configdict = {}
    try:
        with open (configpath, 'r') as j:
            config = json.load(j)
    
        configdict['host'] = str(config['qcluster']['url'])
        configdict['user'] = str(config['qcluster']['user'])
        configdict['password'] = str(config['qcluster']['password'])
        configdict['port'] = 8000
        configdict['fsmap'] = config['qcluster']['fsmap']

    except Exception, excpt:
        print "Improperly formatted {} or missing file: {}".format(configpath, excpt)
        sys.exit(1)

    return configdict


def login(configdict):
    '''Obtain credentials from the REST server'''
    try:
        rc = RestClient(configdict['host'], configdict['port'])
        rc.login(configdict['user'], configdict['password'])
    except Exception, excpt:
        print "Error connecting to the REST server: %s" % excpt
        print __doc__
        sys.exit(1)
    return rc

def getqpath(mountpath, fsmap):
    mapkey = ''
    for prefix in fsmap.keys():
        if mountpath.startswith(prefix) and len(prefix) > len(mapkey):
            mapkey = prefix
    qpath = mountpath.replace(mapkey, fsmap[mapkey])
    return qpath

def getsizeinbytes(size):
    suffixdict = {'b': 512, 'c': 1, 'w': 2, 'k': 1024, 'M': 1048576, 'G': 1073741824}
    if size[-1].isdigit():
        rawsize = args.size
    elif size[-1] in suffixdict.keys(): 
        rawsize = size[:-1]
        modifier = suffixdict(size[-1])
    else:
        print('Size specified incorrectly. Please use integer followed by suffix {}'.format(suffixdict.keys()))
        sys.exit(1)
    try: 
        sizeinbytes = int(rawsize) * modifier
    except Exception, excpt:
        print('Size specified incorrectly: {}'.format(excpt))
        sys.exit(1)

    return sizeinbytes

def translatedate(qdate):
    date = datetime.strptime(qdate, '%Y-%m-%dT%H:%M:%S')
    return date

def findmoddate(samplefile):
    try:
        rawdate = fileapi.get_attr(samplefile)['modification_time'][:-1]
        compdate = translatedate(rawdate)
    except Exception, excpt:
        print('Error in finding date for -newer file {}: {}'.format(samplefile, excpt))
        sys.exit(1)
    return compdate

def testitem(item):
    if (sname is None or re.match(sname, item['name'])) \
    and (ssize is None or int(item['size']) == ssize) \
    and (sdate is None or sdate < findmoddate(item['modification_time'])):
        mntfilepath = item['path'].replace(qsrcpath, mountsrcpath) 
        print(mntfilepath)

def mpselect(itemlist, processes):
    pool = Pool(processes=processes)
    pool.map_async(testitem, itemlist)
    pool.close()
    pool.join()

def iterateoverdir(pathtoread):
    rc = login(configdict)    
    fs = rc.fs
    itemlist = []
    dirobj = fs.read_entire_directory(path=pathtoread, page_size=100000000)
    for item in dirobj.next()['files']:
        itemlist.append(item)
    return itemlist

def finddirs(itemlist):
    dirlist = []
    for item in itemlist:
        if item['type'] == 'FS_FILE_TYPE_DIRECTORY':
            dirlist.append(item['path'])
    return dirlist

def mpdirlist(dirlist, processes):
    sublist = []
    dpool = Pool(processes=processes)
    dpool.map_async(iterateoverdir, dirlist, callback=sublist.extend)
    dpool.close()
    dpool.join()
    return sublist

def processitemlist(itemlist, processes):
    dirlist = finddirs(itemlist)
    if dirlist is not None:
        sublist = mpdirlist(dirlist, processes)
        for sub in sublist:
            if len(sub) is not 0:
                resultlist.extend(sub)
                processitemlist(sub, processes)

def main(argv):
    parser = argparse.ArgumentParser(description="Qumulo API based find command with (very) limited subset of GNU find flags implemented")
    parser.add_argument('sourcepath', type=str)
    parser.add_argument('-name', type=str, default=None)
    parser.add_argument('-newer', type=str, default=None)
    parser.add_argument('-size', type=str, default=None)
    parser.add_argument('--config', type=str, default=configpath)
    parser.add_argument('--processes', type=int, default=1)
    args = parser.parse_args()
    
    global configdict
    configdict = getconfig(args.config)
    global rc
    global fs
    rc = login(configdict)    
    fs = rc.fs

    
    global qsrcpath
    global mountsrcpath
    global sname
    global ssize
    global sdate

    mountsrcpath = os.path.realpath(args.sourcepath)
    qsrcpath = getqpath(mountsrcpath, configdict['fsmap'])

    if args.name is not None:
        sname = fnmatch(args.name)
    else:
        sname = None

    if args.newer is not None:
        sdate = findmoddate(args.newer)
    else:
        sdate = None

    if args.size is not None:
        ssize = getsizeinbytes(args.size)
    else:
        ssize = None
    
    global resultlist
    resultlist = iterateoverdir(qsrcpath)
    processitemlist(resultlist, args.processes)
    #mpselect(resultlist, args.processes)
    for item in resultlist:
        testitem(item)

if __name__ == '__main__':
    main(sys.argv[1:])
