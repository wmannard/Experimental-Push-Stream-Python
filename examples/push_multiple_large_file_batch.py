#!/usr/bin/env python
#-------------------------------------------------------------------------------------
# Push Multiple large files using the batch api
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

def createDoc( myfile ):
    #Create a document
    mydoc = Document('file:///'+myfile)
    #Get the file contents and compress it
    mydoc.GetFileAndCompress( myfile )
    #Set Metadata
    mydoc.AddMetadata("connectortype","CSV")
    authors = []
    authors.append( "Coveo" )
    authors.append( "R&D" )
    #rssauthors is a MultiFacet field
    mydoc.AddMetadata("rssauthors", authors)
    mydoc.Title = "THIS IS A TEST"
    #Set permissions
    user_email = "wim@coveo.com"
    myperm = CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, "", user_email )
    mydoc.SetAllowedAndDeniedPermissions([myperm],[],True)
    return mydoc

def main():
    sourceId = 'xkny6nnnc5il65t2bz44xokl2q-sewimnijmeijer01'
    orgId = 'sewimnijmeijer01'
    apiKey = 'xx2179fe3e-6efc-4817-916e-d83de72e25f0'
    updateSourceStatus = True
    deleteOlder = True

    #Setup the push client
    push = CoveoPush.Push( sourceId, orgId, apiKey)
    #Create a batch of documents
    batch=[]
    batch.append(createDoc('testfiles\\BigExample.pdf'))
    batch.append(createDoc('testfiles\\BigExample2.pptx'))

     #Push the documents
    push.AddDocuments( batch, [], updateSourceStatus, deleteOlder)

    
if __name__ == '__main__':
    main()
