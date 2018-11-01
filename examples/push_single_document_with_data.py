#!/usr/bin/env python
#-------------------------------------------------------------------------------------
# Push Single document with Data property
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
# Needed for the import of the csv


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
    #Get a first Ordering Id
    startOrderingId = push.CreateOrderingId()

    #Create a document
    mydoc = Document('https://myreference&id=TESTME')
    #Set plain text
    mydoc.SetData( "ALL OF THESE WORDS ARE SEARCHABLE")
    #Set FileExtension
    mydoc.FileExtension = ".html"
    #Add Metadata
    mydoc.AddMetadata("connectortype", "CSV")
    authors = []
    authors.append( "Coveo" )
    authors.append( "R&D" )
    #rssauthors is a MultiFacet field
    mydoc.AddMetadata("rssauthors", authors)
    #Set the Title
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

    #Delete older documents
    push.DeleteOlderThan(startOrderingId)
    
if __name__ == '__main__':
    main()
