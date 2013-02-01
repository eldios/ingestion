#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Usage: python poll_images.py <profiles-glob> <enrichment-service-URI.

from amara.thirdparty import json, httplib2
from amara.lib.iri import join
import logging
import logging.handlers
import logging.config
from StringIO import StringIO
import pprint
import sys
import re
import hashlib
import os
import os.path
import urllib


# Used by the logger.
SCRIPT_NAME = "thumbnails downloader"

# Used for logging nice json in the error logs.
# This is used for debugging as well.
pp = pprint.PrettyPrinter(indent=4)

# Used for searching for the thumbnail URL.
URL_FIELD_NAME = u"preview_source_url"

# Used for storing the path to the local filename.
URL_FILE_PATH = u"preview_file_path"


def generate_file_path(id, file_number, file_extension):
    """
    Generates and returns the file path based in provided params.

    Algorithm:

      The file path is generated using the following algorithm:

        -   convert all not allowed characters from the document id to "_"
        -   to the above string add number and extension getting FILE_NAME
        -   calculate md5 from original id
        -   convert to uppercase
        -   insert "/" between each to characters of this hash getting CALCULATED_PATH
        -   join the MAIN_PATH, CALCULATED_PATH and FILE_NAME

    Arguments:
        id             - document id from couchdb  
        file_number    - the number of the file added just before the extension
        file_extension - extension of the file

    Returns:
        filepath       - path, without file name
        full_filepath  - path, with file name

    Example:
        Function call:
            generate_file_path('clemsontest--hcc001-hcc016', 1, "jpg")

        Generated values for the algorithm steps:

        CLEARED_ID: clemsontest__hcc001_hcc016
        FILE_NAME:  clemsontest__hcc001_hcc016_1.jpg
        HASHED_ID:  8E393B3B5DA0E0B3A7AEBFB91FE1278A
        PATH:       8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/
        FULL_NAME:  /tmp/szymon/main_pic_dir/8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/clemsontest__hcc001_hcc016_1.jpg
    """

    logging.debug("Generating filename for document")

    cleared_id = re.sub(r'[-]', '_', id)
    logging.debug("Cleared id: " + cleared_id)
    
    fname = "%s_%s.%s" % (cleared_id, file_number, file_extension)
    logging.debug("File name:  " + fname)
    
    md5sum = hashlib.md5(id).hexdigest().upper()
    logging.debug("Hashed id:  " + md5sum)
    
    path = re.sub("(.{2})", "\\1" + os.sep, md5sum, re.DOTALL)
    logging.debug("PATH:       " + path)
    
    path = os.path.join(conf['THUMBS_ROOT_PATH'], path)
    full_fname = os.path.join(path, fname)
    logging.debug("FULL PATH:  " + full_fname)

    return (path, full_fname)


def download_image(url, id, file_number=1):
    """
    Downloads the thumbnail from the given url and stores it on disk.

    Current implementation stores the file on disk

    Params:
        url         - the url of the file for downloading
        id          - document id, used for the file name generation
        file_number - number of the file for this document

    Returns:
        Name of the file where the image was stored - if everything is OK
        False       - otherwise
    """

    # Get the thumbnail extension from the URL, needed for storing the 
    # file on disk with proper extension.
    fileName, fileExtension = os.path.splitext(url)
    file_extension = fileExtension[1:]

    # Get the directory path and file path for storing the image.
    (path, fname) = generate_file_path(id, file_number, file_extension)
    
    # Let's create the directory for storing the file name.
    if not os.path.exists(path):
        logging.info("Creating directory: " + path)
        os.makedirs(path)
    else:
        logging.debug("Path exists")

    # Open connection to the image using provided URL.
    conn = urllib.urlopen(url)
    if not conn.getcode() / 100 == 2:
        msg = "Got %s from url: [%s] for document: [%s]" % (conn.getcode(), url, id)
        logging.error(msg)
        return False

    # Download the image.
    try:
        logging.info("Downloading file to: " + fname)
        local_file = open(fname, 'w')
        local_file.write(conn.read())
    except Exception as e:
        msg = traceback.format_exception(*sys.exc_info())
        logging.error(msg)
        return False
    else:
        conn.close()
        local_file.close()
    logging.debug("File downloaded")
    return fname


def parse_documents(documents):
    """
    Parses the provided string with json into object.

    Arguments:
        documents String - documents from couchdb in string format

    Returns:
        Object with parsed json.
    """
    io = StringIO(documents)
    return json.load(io)


def process_document(document):
    """
    Processes one document.

    * gets the image url from document
    * downloads the thumbnail
    * updates the document in couchdb

    Arguments:
        document Object - document already parsed

    Returns:
        None
    """
    id = document[u"id"]
    url = document[u'value'][URL_FIELD_NAME]
    logging.info("Processing document id = " + document["id"])
    logging.info("Found thumbnail URL = " + url)

    filepath = download_image(url, id)
    if filepath: 
        # so everything is OK and the file is on disk
        doc = update_document(document, filepath)
        save_document(doc)


def update_document(document, filepath):
    """
    Updates the document setting a filepath to a proper variable.

    Arguments:
        document Object - document for updating (decoded by json module)
        filepath String - filepath to insert

    Returns:
        The document from parameter with additional field containing the filepath.
    """
    document[u'value'][URL_FILE_PATH] = filepath
    return document


def save_document(document):
    """
    Saves the document in the couchdb.

    Arguments:
        document - document to save

    Returns:
        If saving succeeded: the value returned by akara.
        If saving failed: a bunch of error logs is written - returns False.

    """
    logging.info("Updating document in database")
    h = httplib2.Http()
    h.force_exception_as_status_code = True
    url = join(conf['AKARA_SERVER'], conf['UPDATE_DOCUMENT_URL'], document[u'id'])
    logging.debug("Calling url: " + url)
    doc = json.dumps(document[u'value'])
    resp, content = h.request(url, 'POST', body=doc)
    if str(resp.status).startswith('2'):
        return content
    else:
        logging.error("Couldn't update document [id=%s]" % (document[u'id']))
        logging.error("    … with data: %s" % (pp.pformat(document)))
        logging.error("    … with raw data: %s" % (doc,))
        return False


def configure_logger(config_file):
    """
    Configures logging for the script.


    Currently this is a very simple imeplemtation,
    it just reads the configuration from a file.

    Arguments:
        config_file String - path to the config file.

    Returns:
        Nothing, however there is an exception thrown if the file is missing,
        or there is something wrong with it.
    """
    logging.config.fileConfig(config_file)


def process_config(config_file):
    """
    Reads the config file and parses options.

    Arguments:
        config_file String - path to the config file

    Returns:
        Dictionary with values read from the config file.
    """
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    res = {}
    # the names of config settings expected to be in the config file
    names = ['AKARA_SERVER', 'GET_DOCUMENTS_URL', 'GET_DOCUMENTS_LIMIT', \
             'THUMBS_ROOT_PATH', 'UPDATE_DOCUMENT_URL', \
             ]
    for name in names:
        res[name] = config.get('thumbs', name)
    return res


def get_documents():
    """
    Downloads a set of documents from couchdb. If there is an error with 
    downloading the docuemtns, the script exits.

    Arguments:
        None

    Returns:
        None
    """
    logging.info('Getting documents from akara.')
    h = httplib2.Http()
    h.force_exception_as_status_code = True
    url = join(conf['AKARA_SERVER'], conf['GET_DOCUMENTS_URL']) + "?limit=%s" % conf['GET_DOCUMENTS_LIMIT']
    logging.debug('Using akara url: ' + url)
    resp, content = h.request(url, 'GET')
    if str(resp.status).startswith('2'):
        return content
    else:
        logging.error("Couldn't get documents using: " + url)
        logging.error("Emergency exit…")
        exit(1)


def download_thumbs():
    """
    This is the main script function.

    * Downloads documents from couchdb.
    * Downloads images.
    * Updates the documents.

    Arguments:
        None

    Returns:
        None
    """
    # Get documents from couchdb
    documents = get_documents()

    # Convert couchdb reply to json.
    documents = parse_documents(documents)
    logging.info("Got %d documents from akara." % len(documents["rows"]))

    # Process all documents.
    for doc in documents["rows"]:
        process_document(doc)


def parse_cmd_params():
    """
    Parses options for the script.

    Arguments:
        None

    Returns:
        (options, args) - pure output from parser.parse_args()
    """
    from optparse import OptionParser
    parser = OptionParser()
    DEFAULT_CONFIG_FILE = 'dpla-thumbs.ini'
    DEFAULT_LOGGER_CONFIG_FILE = 'thumbs.logger.config'
    parser.add_option("-c", "--config", 
                                dest="config_file",
                                help="Config file, if nothing provided, then '%s' will be used." % DEFAULT_CONFIG_FILE, 
                                default=DEFAULT_CONFIG_FILE)
    parser.add_option("-l", "--logger",
                                dest="logger_file", 
                                help="File with logger configuration, if nothing provided, then %s is used." % DEFAULT_LOGGER_CONFIG_FILE, 
                                default=DEFAULT_LOGGER_CONFIG_FILE)
    return parser.parse_args()


def validate_params(options, args):
    """
    Validates if provided paramters are OK.

    Checks if the provided params exist.
    Checks if the config files exist.

    Exits program if any of the rules is violated.

    Arguments:
        options - object returned by the parse_cmd_params()
        args    - object returned by the parse_cmd_params()

    Returns:
        None

    """
    # Logger is not yet configured:
    print ("Using configuration file: %s" % (options.config_file,))
    print ("Using logger configuration file: %s" % (options.logger_file,))

    def check_file_exists(filename):
        from os.path import isfile
        if not isfile(filename):
            print "There is no file %s" % filename
            print "exiting, good bye…"
            exit(1)

    check_file_exists(options.config_file)
    check_file_exists(options.logger_file)

#################################################################################
if __name__ == '__main__':

    # Parse program params.
    (options, args) = parse_cmd_params()

    # Validate the params.
    validate_params(options, args)

    # Process the script config file.
    conf = process_config(options.config_file)

    # Set up the logger.
    configure_logger(options.logger_file)

    logging.info("Script started.")

    # Start processing thumbnails.
    download_thumbs()
