#!/usr/bin/env python
#-------------------------------------------------------------------------------------
# Push (small) Single document from a filestore
#-------------------------------------------------------------------------------------

import json
import re
import csv
import urllib
import sys
import time
import zlib
import base64
import requests
import datetime



from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush import CoveoConstants


def main():
    sourceId = 'xkny6nnnc5il65t2bz44xokl2q-sewimnijmeijer01'
    orgId = 'sewimnijmeijer01'
    apiKey = 'xx2179fe3e-6efc-4817-916e-d83de72e25f0'

    #Setup the push client
    push = CoveoPush.Push( sourceId, orgId, apiKey)

    myfile = 'testfiles\\Example.pptx'
    #Create a document
    mydoc = Document('file:///'+myfile)
    #Get the file contents and add it to the document
    mydoc.GetFileAndCompress( myfile )
    #Set the metadata
    mydoc.AddMetadata("connectortype","CSV")
    authors = []
    authors.append( "Coveo" )
    authors.append( "R&D" )
    #rssauthors is a MultiFacet field
    mydoc.AddMetadata("rssauthors", authors)
    #Set the title
    mydoc.Title = "THIS IS A TEST"
    #Set permissions
    user_email = "wim@coveo.com"
    #Create a permission Identity
    myperm = CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, "", user_email )
    #Set the permissions on the document
    allowAnonymous = True
    mydoc.SetAllowedAndDeniedPermissions([myperm], [], allowAnonymous)

    #Push the document
    push.AddSingleDocument(mydoc)

    
if __name__ == '__main__':
    main()
