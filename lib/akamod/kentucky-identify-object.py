import os
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


@simple_service('POST', 'http://purl.org/la/dp/identify_preview_location',
    'kentucky-identify-object', 'application/json')
def kentucky_identify_object(body, ctype, rights_field="aggregatedCHO/rights", download="True"):
    """
    Responsible for: adding a field to a document with the URL where we
    should expect to the find the thumbnail
    """

    LOG_JSON_ON_ERROR = True

    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

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

    url = None
    try:
        url = getprop(data, "aggregatedCHO/relation")
        logger.debug("Found URL: " + url)
    except KeyError as e:
        msg = e.args[0]
        logger.error(msg)
        return body

    base_url, ext = os.path.splitext(url)  

    # Thumb url field.
    thumb_url = "%s_tb%s" % (base_url, ext)

    # Get the rights field
    rights = None
    try:
        rights = getprop(data, rights_field)
    except KeyError as e:
        msg = e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    ob = {"@id": thumb_url, "format": "", "rights": rights}

    data["object"] = ob

    status = "ignore"
    if download == "True":
        status = "pending"

    if "admin" in data:
        data["admin"]["object_status"] = status
    else:
        data["admin"] = {"object_status": status}

    logger.debug("Thumbnail URL = " + thumb_url)
    return json.dumps(data)
