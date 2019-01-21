#!/usr/bin/env python
# -------------------------------------------------------------------------------------
# Push Multiple large files using the batch api
# -------------------------------------------------------------------------------------

import os

from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush import CoveoConstants


def createDoc(myfile):
    # Create a document
    mydoc = Document('file:///'+myfile)
    # Get the file contents and compress it
    mydoc.GetFileAndCompress(myfile)
    # Set Metadata
    mydoc.AddMetadata("connectortype", "CSV")
    authors = []
    authors.append("Coveo")
    authors.append("R&D")
    # rssauthors should be set as a multi-value field in your Coveo Cloud organization
    mydoc.AddMetadata("rssauthors", authors)
    mydoc.Title = "THIS IS A TEST"
    # Set permissions
    user_email = "wim@coveo.com"
    myperm = CoveoPermissions.PermissionIdentity(
        CoveoConstants.Constants.PermissionIdentityType.User, "", user_email)
    mydoc.SetAllowedAndDeniedPermissions([myperm], [], True)
    return mydoc


def main():
    sourceId = os.environ.get('PUSH_SOURCE_ID') or '--Enter your source id--'
    orgId = os.environ.get('PUSH_ORG_ID') or '--Enter your org id--'
    apiKey = os.environ.get('PUSH_API_KEY') or '--Enter your API key--'

    updateSourceStatus = True
    deleteOlder = True

    # Setup the push client
    push = CoveoPush.Push(sourceId, orgId, apiKey)
    # Create a batch of documents
    batch = [
        createDoc(os.path.join('testfiles', 'BigExample.pdf')),
        createDoc(os.path.join('testfiles', 'BigExample2.pptx'))
    ]

    # Push the documents
    push.AddDocuments(batch, [], updateSourceStatus, deleteOlder)


if __name__ == '__main__':
    main()
