#!/usr/bin/env python
# -------------------------------------------------------------------------------------
# Push Single, large document from a filestore
# Automatically a AWS s3 Upload will be retrieved, the file will be uploaded and be pushed
# -------------------------------------------------------------------------------------

import os

from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush import CoveoConstants


def main():
    sourceId = os.environ.get('PUSH_SOURCE_ID') or '--Enter your source id--'
    orgId = os.environ.get('PUSH_ORG_ID') or '--Enter your org id--'
    apiKey = os.environ.get('PUSH_API_KEY') or '--Enter your API key--'

    # Setup the push client
    push = CoveoPush.Push(sourceId, orgId, apiKey)

    myfile = os.path.join('testfiles', 'BigExample.pdf')
    # Create a document
    mydoc = Document('file:///' + myfile)
    # Get the file contents and add it to the document
    mydoc.GetFileAndCompress(myfile)
    # Set the metadata
    mydoc.AddMetadata("connectortype", "PDF")
    authors = []
    authors.append("Coveo")
    authors.append("R&D")
    # rssauthors should be set as a multi-value field in your Coveo Cloud organization
    mydoc.AddMetadata("rssauthors", authors)
    # Set the title
    mydoc.Title = "THIS IS A TEST"
    # Set permissions
    user_email = "wim@coveo.com"
    # Create a permission Identity
    myperm = CoveoPermissions.PermissionIdentity(
        CoveoConstants.Constants.PermissionIdentityType.User, "", user_email)
    # Set the permissions on the document
    allowAnonymous = True
    mydoc.SetAllowedAndDeniedPermissions([myperm], [], allowAnonymous)

    # Push the document
    push.AddSingleDocument(mydoc)


if __name__ == '__main__':
    main()
