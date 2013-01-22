import re
from zen import dateparser
from dateutil.parser import parse as dateutil_parse
import timelib

# default date used by dateutil-python to populate absent date elements during parse,
# e.g. "1999" would become "1999-01-01" instead of using the current month/day
DEFAULT_DATETIME = dateutil_parse("2000-01-01")
DATE_RANGE_RE = r'(\S+)\s*-\s*(\S+)'
DATE_8601 = '%Y-%m-%d'

def split_date(d):
    range = [robust_date_parser(x) for x in re.search(DATE_RANGE_RE,d).groups()]
    return range

def robust_date_parser(d):
    """
    Robust wrapper around some date parsing libs, making a best effort to return
    a single 8601 date from the input string. No range checking is performed, and
    any date other than the first occuring will be ignored.

    We use timelib for its ability to make at least some sense of invalid dates,
    e.g. 2012/02/31 -> 2012/03/03

    We rely only on dateutil.parser for picking out dates from nearly arbitrary
    strings (fuzzy=True), but at the cost of being forgiving of invalid dates
    in those kinds of strings.

    Returns None if it fails
    """
    dd = None
    try:
        dd = dateutil_parse(d,fuzzy=True,default=DEFAULT_DATETIME)
    except:
        try:
            dd = timelib.strtodatetime(d)
        except ValueError:
            pass

    if dd:
        ddiso = dd.isoformat()
        return ddiso[:ddiso.index('T')]

    return dateparser.to_iso8601(d.replace('ca.','').strip()) # simple cleanup prior to parse

def parse_date_or_range(d):
    if ' - ' in d: # FIXME could be more robust here, e.g. use year regex
        a,b = split_date(d)
    else:
        parsed = robust_date_parser(d)
        a,b = parsed,parsed
    return a,b

DATE_TESTS = {
    "ca. July 1896": ("1896-07-01","1896-07-01"), # fuzzy dates
    "1999.11.01": ("1999-11-01","1999-11-01"), # period delim
    "2012-02-31": ("2012-03-02","2012-03-02"), # invalid date cleanup
    "12-19-2010": ("2010-12-19","2010-12-19"), # M-D-Y
    "5/7/2012": ("2012-05-07","2012-05-07"), # slash delim MDY
    "1999 - 2004": ("1999-01-01","2004-01-01"), # year range
    " 1999   -   2004  ": ("1999-01-01","2004-01-01"), # range whitespace
}
def test_parse_date_or_range():
    for i in DATE_TESTS:
        res = parse_date_or_range(i)
        assert res == DATE_TESTS[i], "For input '%s', expected '%s' but got '%s'"%(i,DATE_TESTS[i],res)
