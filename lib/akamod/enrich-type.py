from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich-type', 'enrich-type', 'application/json')
def enrichtype(body,ctype,action="enrich-type",prop="type"):
    '''   
    Service that accepts a JSON document and enriches the "type" field of that document
    by: 

    a) making the type lowercase
    b) converting "image" to "still image" (TODO: Amy to confirm that this is ok)
    b) applying a set of regexps to do data cleanup (remove plural forms)
    
    By default works on the 'type' field, but can be overridden by passing the name of the field to use
    as a parameter
    '''   

    REGEXPS = ('images','image'), ('still image','image')

    def cleanup(s):
        s = s.lower().strip()
        for pattern, replace in REGEXPS:
            s = re.sub(pattern, replace, s)
        return s

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type','text/plain')
        return "Unable to parse body as JSON"

    if prop in data:
        result = []
        for s in (data[prop] if not isinstance(data[prop],basestring) else [data[prop]]):
            result.append(cleanup(s))

        data[prop] = result[0] if len(result) == 1 else result

    return json.dumps(data)
