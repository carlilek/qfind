#!/usr/bin/env python
import os
import sys
import qumulo.lib.auth
import qumulo.lib.opts
import qumulo.lib.request
import qumulo.rest.fs as fs
import arrow

#Import credentials
user = os.environ.get('QUMULO_USER') or 'admin'
passwd = os.environ.get('QUMULO_PWD')
host = os.environ.get('QUMULO_CLUSTER')
if None in (user, passwd, host):
    print "Please set environment variables QUMULO_CLUSTER, QUMULO_USER, and QUMULO_PWD"
    sys.exit(0)
port = 8000


conninfo = qumulo.lib.request.Connection(host, int(port))
login_results, _ = qumulo.rest.auth.login(conninfo, None, user, passwd)
creds = qumulo.lib.auth.Credentials.from_login_response(login_results)


def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def get_dir_info(conninfo, creds, sourcepath, monthago):
    dir_attrs = []
    contents_files = []
    contents_dirs = []
    logfiles = {}
    all_dir_attrs=fs.read_entire_directory(conninfo, creds, page_size=100000000, path=sourcepath)
    for dir_attrs in all_dir_attrs:
        item_attrs = dir_attrs.lookup("files")
        for item in item_attrs:
            if item["type"] == "FS_FILE_TYPE_DIRECTORY":
                contents_dirs.append(item)
            if ".log" in item["name"]:
                logattr = fs.get_file_attr(conninfo, creds, path=item["path"])
                if monthago > arrow.get(logattr[0]["modification_time"]):
                    logfiles[item["path"]] = logattr[0]["size"]
    return contents_dirs, logfiles

def parsedirs(directories, monthago):
    size_sge = {}
    final_pathsizes = {}
    for directory in directories:
        dmtime = arrow.get(fs.get_file_attr(conninfo,creds,directory["path"])[0]["modification_time"])
        if directory["name"] in ("sge_error", "sge_output") and monthago > dmtime:
            d = fs.read_dir_aggregates(conninfo, creds, directory["path"])
            size_sge[d[0]["path"]] = d[0]["total_data"]
        else:
            subdirs, sublfs = get_dir_info(conninfo, creds, directory["path"], monthago)
            subsizes = parsedirs(subdirs, monthago)
        final_pathsizes = merge_dicts(final_pathsizes, sublfs, size_sge, subsizes)
    return final_pathsizes



def main(argv):
    sourcepath=sys.argv[1]
    monthago=arrow.now().replace(months=-1)
    directories, logfiles=get_dir_info(conninfo, creds, sourcepath, monthago)
    size_paths = parsedirs(directories, monthago)
    
    totalsize = 0
    for eachsize in size_paths.values():
        totalsize = float(eachsize) + totalsize
    print "_______________________"
    print sourcepath
    print "{} MB".format(totalsize / 1024 / 1024)
    print "{} files found.".format(len(size_paths.keys()))
    print "_______________________"

if __name__ == '__main__':
    main(sys.argv[1:])
