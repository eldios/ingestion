from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/shred', 'shred', 'application/json')
def shred(body,ctype,action="shred",prop=None,delim=','):
    '''   
    Service that accepts a JSON document and "shreds" or "unshreds" the value
    of the field(s) named by the "prop" parameter

    "prop" can include multiple property names, delimited by a comma (the delim
    property is used only for the fields to be shredded/unshredded). This requires
    that the fields share a common delimiter however.
    '''   
    
    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    for p in prop.split('.'):
        if exists(data,p):
            v = getprop(data,p)
            if action == "shred":
                if isinstance(v,list):
                    v = delim.join(data[p])
                    setprop(data,p,v)
                if delim not in v: continue
                setprop(data,p,[ s.strip() for s in v.split(delim) ])
            elif action == "unshred":
                if isinstance(v,list):
                    setprop(data,p,delim.join(v))

    return json.dumps(data)
