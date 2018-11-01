#!/usr/bin/env python
#-------------------------------------------------------------------------------------
# Delete Single document 
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

    #First add the document
    mydoc = Document('https://myreference&id=TESTME')
    #Set plain text
    mydoc.SetData( "ALL OF THESE WORDS ARE SEARCHABLE")
    #Set FileExtension
    mydoc.FileExtension = ".html"
    #Add Metadata
    mydoc.AddMetadata("connectortype", "CSV")
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

    time.sleep(100)

    #Remove it
    push.RemoveSingleDocument('https://myreference&id=TESTME')
    
if __name__ == '__main__':
    main()
