import sys
from server_support import server, print_error_log
from amara.thirdparty import httplib2
from amara.thirdparty import json

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()
url = server() + "copy_rights"

def _get_server_response(body):
    return H.request(url,"POST",body=body,headers=CT_JSON)

def test_copy_rights1():
    """Should do nothing"""

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_rights2():
    """Should do nothing"""

    INPUT = {
        "key1": "value1",
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }

    resp,content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content) == INPUT

def test_copy_rights3():
    """Should copy rights to isShownAt"""

    INPUT = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": ""
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "key2": "value2"
    }
    resp,content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_copy_rights4():
    """Should copy rights to isShownAt and aggregatedCHO/hasView items"""

    INPUT = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": ""
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": ""
                },
                {
                    "key1": "value1",
                    "rights": ""
                }
            ]
        },
        "key2": "value2"
    }
    EXPECTED = {
        "key1": "value1",
        "isShownAt": {
            "key1": "value1",
            "key2": "value2",
            "rights": "These are the rights"
        },
        "aggregatedCHO": {
            "key1" : "value1",
            "key2": "value2",
            "rights": "These are the rights",
            "hasView": [
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                },
                {
                    "key1": "value1",
                    "rights": "These are the rights"
                }
            ]
        },
        "key2": "value2"
    }
    resp,content = _get_server_response(json.dumps(INPUT))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
