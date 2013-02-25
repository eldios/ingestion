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
   "edm": "http://www.europeana.eu/schemas/edm/",
   "LCSH": "http://id.loc.gov/authorities/subjects",
   "name": "xsd:string",
   "collection" : "dpla:aggregation",
   "aggregatedDigitalResource" : "dpla:aggregatedDigitalResource",
   "originalRecord" : "dpla:originalRecord",
   "state": "dpla:state",                             
   "coordinates": "dpla:coordinates",
   "stateLocatedIn" : "dpla:stateLocatedIn",
   "aggregatedCHO" : "edm:aggregatedCHO",
   "dataProvider" : "edm:dataProvider",
   "hasView" : "edm:hasView",
   "isShownAt" : "edm:isShownAt",
   "object" : "edm:object",
   "provider" : "edm:provider",
   "begin" : {
     "@id" : "dpla:dateRangeStart",     
     "@type": "xsd:date"
   },
   "end" : {
     "@id" : "dpla:end",
     "@type": "xsd:date"
   }
}

def date_transform(d,groupkey,itemKey):
    date = {}
    if isinstance(itemKey,list):
        date["begin"] = arc_group_extraction(groupKey,itemKey[0])
        date["end"] = arc_group_extraction(groupKey,itemKey[1])
        date["displayDate"] = date["begin"]
    else:
        date["displayDate"] = arc_group_extraction(group,itemKey)
        date["begin"] = date["displayDate"]
        date["end"] = date["displayDate"]

    return {"date": date}

def is_part_of_transform(d):
    is_part_of = []
    hiers = ['series','file unit']
    hierarchy = getprop(d, hierarchy)
    for hier in hierarchy:
        if hier["hierarchy-item-lod"].lower() in hiers:
            is_part_of.append({"name": hier["hierarchy-item-title"]})

    return {"isPartOf": is_part_of}
            
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

    # Then, check out the "coverage" field, since dates may be there
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

def rights_transform(d):
    rights = []

    if "access-restriction" in d:
        rights.append({"name": getprop(d,"access-restriction/restriction-status",keyErrorAsNone=True)})
    if "use-restriction" in d:
        rights.append({"name": getprop(d,"use-restriction/use-status",keyErrorAsNone=True)})
        #sections.append(getprop(d,"use-restriction/use-status",keyErrorAsNone=True))
        #sections.append(getprop(d,"use-restriction/use-note",keyErrorAsNone=True))

        # FIXME unclear on the structure of this section, and rarely used so defer for now
        #surs = getprop(d,"use-restriction/specific-use-restrictions",keyErrorAsNone=True)
        #if isinstance(surs,dict):
        #    for k,v in surs:
        #        
        #else:
        #    sections.append(sur)

    #return {"rights": "; ".join(filter(None,sections))}
    return {"rights": rights}

def is_shown_at_transform(d):
    object = "http://research.archives.gov/description/" + str(d["arc-id-desc"])
    return {
        "isShownAt" : {
            "@id" : object,
            "format" : d.get("format",None),
            "rights" : "" 
            }
        }

def has_view_transform(d):
    has_view = []

    def add_views(has_view,rge,url,format=None):
        for i in range(0,rge):
            has_view.append({
                "url": url[i],
                "rights": "",
                "format": format[i] if format else ""
            })
        return has_view

    if "objects" in d:
        groupKey = "objects"
        itemKey = "object"
        urlKey = "file-url"
        formatKey = "mime-type"
        url = arc_group_extraction(d,groupKey,itemKey,urlKey)
        format = arc_group_extraction(d,groupKey,itemKey,formatKey)
        has_view = add_views(has_view,len(url),url,format)

    if "online-resources" in d:
        groupKey = "online-resources"
        itemKey = "online-resource"
        urlKey = "online-resource-url"
        url = arc_group_extraction(d,groupKey,itemKey,urlKey)
        has_view = add_views(has_view,len(url),url)

    return {"hasView": has_view}

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

    "groupKey" is the name of the containing property, e.g. "creators"
    "itemKey" is the name of the contained property, e.g. "creator"
    "nameKey" is the property of the itemKey-named resource to be extracted
      if present, otherwise the value of the nameKey property
    """
    group = d.get(groupKey)
    if not group:
        data = None
    else:
        data = []
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
        if not isinstance(subitem,basestring):
            for s in (subitem if isinstance(subitem,list) else [subitem]):
                if nameKey:
                    data.append(s.get(nameKey,None))
                else:
                    data.append(s)
        else:
            data.append(subitem)

    return data

# Structure mapping the original top level property to a function returning a single
# item dict representing the new property and its value
CHO_TRANSFORMER = {
    "contributor"           : lambda d: {"contributor": arc_group_extraction(d,"contributors","contributor","contributor-display")},
    "creators"              : lambda d: {"creator": arc_group_extraction(d,"creators","creator","creator-display")},
    "release-dates"         : dates_transform(d,"release-dates","release-date"),
    "broadcast-dates"       : dates_transform(d,"broadcast-dates","broadcast-date"),
    "production-dates"      : dates_transform(d,"production-dates","production-date"), 
    "coverage-dates"        : dates_transform(d,"coverage-dates",["cov-start-date","cov-end-date"]),
    "copyright-dates"       : dates_transform(d,"copyright-dates","copyright-date"),
    "scope-content-note"    : lambda d: {"description": d.get("scope-content-note")}, 
    "languages"             : lambda d: {"language": arc_group_extraction(d,"languages","language")}, # needs a lookup table/service. TBD.
    "format"                : lambda d: {"format": arc_group_extraction(d,"media-occurences","media-occurence","media-type")},
    "general-records-types" : lambda d: {"type": arc_group_extraction(d,"general-records-types","general-records-type","general-records-type-desc")},
    #"publisher"            : lambda d: {"publisher": arc_group_extraction(d,"reference-units","reference-unit","name")},
    "hierarchy"             : is_part_of_transform,
    "access-restriction"    : rights_transform,
    "use-restriction"       : rights_transform,
    "subject-references"    : lambda d: {"subject": arc_group_extraction(d,"subject-references","subject-reference","display-name")}, # needs additional LCSH enrichment
    "title"                 : lambda d: {"title": d.get("title-only")},
    "objects"               : has_view_transform,
    "online-resources"      : has_view_transform
}

AGGREGATION_TRANSFORMER = {
    "collection"       : lambda d: {"collection": d.get("collection",None)},
    "id"               : lambda d: {"id": d.get("id"), "@id" : "http://dp.la/api/items/"+d.get("id","")},
    "_id"              : lambda d: {"_id": d.get("_id")},
    "originalRecord"   : lambda d: {"originalRecord": d.get("originalRecord",None)},
    "ingestType"       : lambda d: {"ingestType": d.get("ingestType")},
    "ingestDate"       : lambda d: {"ingestDate": d.get("ingestDate")},
    "arc-id-desc"      : is_shown_at_transform
    #"parent"           : lambda d: {"dataProvider": getprop(d,"parent/parent-title")}
}

@simple_service("POST", "http://purl.org/la/dp/arc-to-dpla", "arc-to-dpla", "application/ld+json")
def arctodpla(body,ctype,geoprop=None):
    """   
    Convert output of JSON-ified ARC (NARA) format into the DPLA JSON-LD format.

    Parameter "geoprop" specifies the property name containing lat/long coords
    """

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type","text/plain")
        return "Unable to parse body as JSON"

    global GEOPROP
    GEOPROP = geoprop

    out = {
        "@context": CONTEXT,
        "aggregatedCHO": {}
    }

    # For ARC, "data" is the source record so set it here
    data["originalRecord"] = deepcopy(data)

    # Apply all transformation rules from original document
    for p in data.keys():
        if p in CHO_TRANSFORMER:
            out["aggregatedCHO"].update(CHO_TRANSFORMER[p](data))
        if p in AGGREGATION_TRANSFORMER:
            out.update(AGGREGATION_TRANSFORMER[p](data))

    # Additional content not from original document

    if "HTTP_CONTRIBUTOR" in request.environ:
        try:
            out["provider"] = json.loads(base64.b64decode(request.environ["HTTP_CONTRIBUTOR"]))
        except Exception as e:
            logger.debug("Unable to decode Contributor header value: "+request.environ["HTTP_CONTRIBUTOR"]+"---"+repr(e))

    # Strip out keys with None/null values?
    out = dict((k,v) for (k,v) in out.items() if v)

    return json.dumps(out)
