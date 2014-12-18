#!/usr/bin/env python
import sys
import re
from datetime import datetime

format = re.compile("^\d{2}/\d{2}/\d{4}\s\S+\|\d+\|AVA")
date_format = "%d/%m/%Y"

def sort_destinations(destinations):
    elements = destinations.split("#")
    if len(elements) == 1:
        return destinations
    elements.sort()
    return "#".join(elements)

for line in sys.stdin:
    # check if it's a valid availability
    if format.match(line) != None:
        try:
            elements = line.split("|")
            if len(elements) == 15:
                request_date_string = elements[0][0:10]
                request_date = datetime.strptime(request_date_string, date_format)
                destination = sort_destinations(elements[4])
                entry_date_string = elements[6]
                entry_date = datetime.strptime(entry_date_string, date_format)
                days_diff_string = str((entry_date - request_date).days)
                number_hotels_returned = elements[13]
                print "%s|%s|%s|%s" % (request_date_string, destination, days_diff_string, number_hotels_returned)
        except:
            pass