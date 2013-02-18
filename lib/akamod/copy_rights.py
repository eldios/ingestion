from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/copy_rights', 'copy_rights', 'application/json')
def copyrights(body,ctype,rights_field="aggregatedCHO/rights"):
    """ Copies rights field value to fields specified in COPYTO
    """

    COPYTO = ["isShownAt", "aggregatedCHO/hasView"]

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, rights_field):
        rights = getprop(data, rights_field)
        for c in COPYTO:
            if exists(data, c):
                field = getprop(data, c)
                if not isinstance(field, list):
                    field = [field]
                for i in range(0,len(field)):
                        setprop(field[i], "rights", rights)

    return json.dumps(data)
