from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/move_dates_to_temporal', 'move_dates_to_temporal', 'application/json')
def movedatestotemporal(body,ctype,action="move_dates_to_temporal",prop=None):
    """
    Service that accepts a JSON document and moves any dates found in the prop field to the
    temporal field.
    """

    if not prop:
        logger.error("No prop supplied")
        return body

    REGSUBS = ("\(", ""), ("\)", "")
    REGMATCHES = [" *\d{4}", "( *\d{1,4} *[-/]){2} *\d{1,4}"]

    def cleanup(s):
        for pattern, replace in REGSUBS:
            s = re.sub(pattern, replace, s)
        return s.strip()

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        p = []
        temporal_field = "aggregatedCHO/temporal"
        temporal = getprop(data, temporal_field) if exists(data, temporal_field) else []

        for d in getprop(data, prop):
            name = cleanup(d["name"])
            for pattern in REGMATCHES:
                if re.match(pattern, name):
                    # If there's a match, let's save the cleaned up value
                    d["name"] = name
                    temporal.append(d)
                    break
            if d not in temporal:
                # Append to p, which will overwrite data[prop]
                p.append(d)

        if temporal:
            setprop(data, temporal_field, temporal)
        if p:
            setprop(data, prop, p)
        else:
            delprop(data, prop)

    return json.dumps(data)
