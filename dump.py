#!/usr/bin/env python
import StringIO
import urllib
import datetime
import time

import elasticsearch as es
import sys
from elasticsearch.helpers import scan
from optparse import OptionParser

def get_index_name():
    datetime.datetime.now().strftime("forseti-%Y%m%d")

usage = "usage: %prog [options] <es_host>"
parser = OptionParser(usage=usage)
parser.add_option("-p", "--partner", help="parter_code to dump, all if not specified")
parser.add_option("-o", "--output", help="the output file, console(stdout) if not specified")
parser.add_option("-n", "--num", help="this max num to retrive, default=10", type="int", default=10)
parser.add_option("-r", "--prefix", help="the url prefix, default(/)", default='/')
parser.add_option("-i", "--index", help="index to query from, default(forseti-yyyymmdd)", default=get_index_name())
parser.add_option("", "--ignore", help="ignore the fields,seperated by comma(,)")

options, args = parser.parse_args()


def print_usage():
    parser.print_help(sys.stdout)


def camel_to_slash(var):
    if var.lower() == var:
        return var

    io = StringIO.StringIO()
    for c in var:
        if 'A' <= c <= 'Z':
            io.write("_")
            io.write(c.lower())
        else:
            io.write(c)

    return io.getvalue()


def generate_url(prefix, params):
    return prefix + "?" + urllib.urlencode(params)


def timestamp(x):
    return int(time.mktime(time.strptime(x, '%Y-%m-%d %H:%M:%S'))) * 1000

def query(es_host, opts):
    client = es.Elasticsearch(es_host)

    match = {}

    q = {
        "match_all": {}
    }

    if opts.partner is not None:
        q = {
            "match": {
                "activity.partnerCode": opts.partner
            }
        }

    return scan(client, query={
        "query": q,
        "partial_fields": {
            "part1": {
                "include": "activity.*"
            }
        }
    }, index=opts.index)


def print_request_url(hits_scanner, options):
    # calc ignored fields
    ignored_fields = set()
    if options.ignore is not None:
        for f in options.ignore.split(","):
            ignored_fields.add(f)

    out_file = sys.stdout
    if options.output is not None:
        out_file = open(options.output, 'w')

    count = options.num
    for hit in hits_scanner:
        if count == 0:
            break
        try:
            activity = hit['fields']['part1'][0]['activity']
            count -= 1
        except Exception, e:
            continue

        new_activity = {}
        for k, v in activity.items():
            field_name = camel_to_slash(k)
            if field_name in ignored_fields:
                continue
            if field_name == 'event_occur_time':
                v = format_datetime(v)
            if type(v) == unicode:
                new_activity[field_name] = v.encode('utf8')
            else:
                new_activity[field_name] = v

        url = generate_url(options.prefix, new_activity)
        out_file.write(url)
        out_file.write("\n")

def format_datetime(x):
    x = int(x) // 1000
    return datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')

if not len(args) == 1:
    print_usage()
    sys.exit(1)

host = args[0]

res = query(host, options)
print_request_url(res, options)

