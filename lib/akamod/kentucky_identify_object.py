import os
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


@simple_service('POST', 'http://purl.org/la/dp/kentucky_identify_object',
    'kentucky_identify_object', 'application/json')
def kentucky_identify_object(body, ctype, rights_field="aggregatedCHO/rights", download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """

    LOG_JSON_ON_ERROR = True

    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    relation_field = "aggregatedCHO/relation"
    if exists(data, relation_field):
        url = getprop(data, relation_field)
    else:
        msg = "Field %s does not exist" % relation_field
        logger.error(msg)
        return body

    base_url, ext = os.path.splitext(url)  
    thumb_url = "%s_tb%s" % (base_url, ext)

    if exists(data, rights_field):
        rights = getprop(data, rights_field)
    else:
        msg = "Field %s does not exist" % rights_field
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    data["object"] = {"@id": thumb_url, "format": "", "rights": rights}

    status = "ignore"
    if download == "True":
        status = "pending"

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    return json.dumps(data)
