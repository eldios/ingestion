from akara import logger
from akara import request, response
from akara.services import simple_service
from amara.lib.iri import is_absolute
from amara.thirdparty import json
from functools import partial
import base64
import sys
import re
from copy import deepcopy
from dplaingestion.temporal import parse_date_or_range
from dplaingestion.selector import getprop

GEOPROP = None

#FIXME not format specific, move to generic module
CONTEXT = {
   "@vocab": "http://purl.org/dc/terms/",
   "dpla": "http://dp.la/terms/",
   "name": "xsd:string",
   "dplaContributor": "dpla:contributor",
   "dplaSourceRecord": "dpla:sourceRecord",
   "coordinates": "dpla:coordinates",
   "state": "dpla:state",                             
   "start" : {
     "@id" : "dpla:start",
     "@type": "xsd:date"
   },
   "end" : {
     "@id" : "dpla:end",
     "@type": "xsd:date"
   },
   "iso3166-2": "dpla:iso3166-2",
   "iso639": "dpla:iso639",
   "LCSH": "http://id.loc.gov/authorities/subjects"
}

def created_transform(d):
    covdates = d.get("coverage-dates")
    if not covdates:
        created = None
    else:
        start = covdates.get('cov-start-date')
        end = covdates.get('cov-end-date')
        created = {}
        if start:
            created['start'] = start
        if end:
            created['end'] = end

        if not start and not end:
            created = None

    return {"created": created}

def temporal_transform(d):
    temporal = []

    # First look at the date field, and log any parsing errors
    for t in (d["date"] if not isinstance(d["date"],basestring) else [d["date"]]):
        a,b = parse_date_or_range(t)
        if a is not None and b is not None:
            temporal.append( {
                "start": a,
                "end": b
            })
        else:
            logger.debug("Could not parse date: " + t)

    # Then, check out the 'coverage' field, since dates may be there
    if "coverage" in d:
        for t in (d["coverage"] if not isinstance(d["coverage"],basestring) else [d["coverage"]]):
            a,b = parse_date_or_range(t)
            if a is not None and b is not None:
                temporal.append( {
                    "start": a,
                    "end": b
                })

    return {"temporal":temporal}

def source_transform(d):
    source = ""
    for s in d["handle"]:
        if is_absolute(s):
            source = s
            break
    return {"source":source}

def subject_transform(d):
    subject = []
    for s in (d["subject"] if not isinstance(d["subject"],basestring) else [d["subject"]]):
        subject.append({
            "name" : s.strip()
        })
    return {"subject" : subject}

def rights_transform(d):
    sections = []
    if 'use-restriction' in d:
        sections.append(getprop(d,"use-restriction/use-status",keyErrorAsNone=True))
        sections.append(getprop(d,"use-restriction/use-note",keyErrorAsNone=True))

        # FIXME unclear on the structure of this section, and rarely used so defer for now
        #surs = getprop(d,"use-restriction/specific-use-restrictions",keyErrorAsNone=True)
        #if isinstance(surs,dict):
        #    for k,v in surs:
        #        
        #else:
        #    sections.append(sur)

    return {'rights': "; ".join(filter(None,sections))}

def arc_group_extraction(d,groupKey,itemKey,nameKey=None):
    """
    Generalization of what proved to be an idiom in ARC information extraction,
    e.g. in the XML structure;
    <general-records-types>
      <general-records-type num="1">
         <general-records-type-id>4237049</general-records-type-id>
         <general-records-type-desc>Moving Images</general-records-type-desc>
      </general-records-type>
    </general-records-types>

    'groupKey' is the name of the containing property, e.g. "creators"
    'itemKey' is the name of the contained property, e.g. "creator"
    'nameKey' is the property of the itemKey-named resource to be extracted
      if present, otherwise the value of the nameKey property
    """
    group = d.get(groupKey)
    if not group:
        data = None
    else:
        # xmltodict converts what would be a list of length 1, into just that
        # lone dict. we have to deal with that twice here.
        # could definitely benefit from being more examplotron-like.
        if isinstance(group,list):
            # FIXME we pick the first in the list. unlikely to be
            # optimal and possibly incorrect in some cases
            item = group[0]
        else:
            item = group

        subitem = item.get(itemKey)
        if isinstance(subitem,list):
            subitem=subitem[0]

        if nameKey:
            data = subitem.get(nameKey,None)
        else:
            # we interpret no nameKey to mean that the value is a string and not a dict
            data = subitem

    return data

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
TRANSFORMER = {
    "contributor"      : lambda d: {"contributor": arc_group_extraction(d,'contributors','contributor','contributor-display')},
    "parent"           : lambda d: {"source": getprop(d,"parent/parent-title")},
    "original_record"  : lambda d: {"dplaSourceRecord": d.get("original_record",None)},
    "coverage-dates"   : created_transform,
    "title"            : lambda d: {"title": d.get("title-only")},
    "creators"         : lambda d: {'creator': arc_group_extraction(d,'creators','creator','creator-display')},
    "publisher"        : lambda d: {'publisher': arc_group_extraction(d,'reference-units','reference-unit','name')},
    "type"             : lambda d: {'type': arc_group_extraction(d,'general-records-types','general-records-type','general-records-type-desc')},
    "format"           : lambda d: {'format': arc_group_extraction(d,'media-occurences','media-occurence','media-type')},
    "description"      : lambda d: {"description": d.get("title")},
    "id"               : lambda d: {"id": d.get("id"), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "use-restriction"  : rights_transform,
    "collection"       : lambda d: {"isPartOf": d.get("collection")},
    "subject"          : subject_transform,
    "ingestType"       : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"       : lambda d: {"ingestDate": d.get("ingestDate")},
    "_id"              : lambda d: {"_id": d.get("_id")}

    # language - needs a lookup table/service. TBD.
    # subject - needs additional LCSH enrichment. just names for now
}

@simple_service('POST', 'http://purl.org/la/dp/arc-to-dpla', 'arc-to-dpla', 'application/ld+json')
def arctodpla(body,ctype,geoprop=None):
    '''   
    Convert output of JSON-ified ARC (NARA) format into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    '''

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
    }

    # For ARC, "data" is the source record so set it here
    data["original_record"] = deepcopy(data)

    # Apply all transformation rules from original document
    for p in data.keys():
        if p in TRANSFORMER:
            out.update(TRANSFORMER[p](data))

    # Additional content not from original document

    if 'HTTP_CONTRIBUTOR' in request.environ:
        try:
            out["dplaContributor"] = json.loads(base64.b64decode(request.environ['HTTP_CONTRIBUTOR']))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ['HTTP_CONTRIBUTOR']+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
