#!/usr/bin/env python
# -------------------------------------------------------------------------------------
# Delete Single document
# -------------------------------------------------------------------------------------

import os
import time

from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush.CoveoConstants import Constants


def main():
    sourceId = os.environ.get('PUSH_SOURCE_ID') or '--Enter your source id--'
    orgId = os.environ.get('PUSH_ORG_ID') or '--Enter your org id--'
    apiKey = os.environ.get('PUSH_API_KEY') or '--Enter your API key--'

    # Setup the push client
    push = CoveoPush.Push(sourceId, orgId, apiKey)

    # First add the document
    mydoc = Document("https://myreference&id=TESTME")
    # Set plain text
    mydoc.SetData("ALL OF THESE WORDS ARE SEARCHABLE")
    # Set FileExtension
    mydoc.FileExtension = ".html"
    # Add Metadata
    mydoc.AddMetadata("connectortype", "HTML")
    # Set the title
    mydoc.Title = "THIS IS A TEST"
    # Set permissions
    user_email = "wim@coveo.com"
    # Create a permission identity
    myperm = CoveoPermissions.PermissionIdentity(Constants.PermissionIdentityType.User, "", user_email)
    # Set the permissions on the document
    allowAnonymous = True
    mydoc.SetAllowedAndDeniedPermissions([myperm], [], allowAnonymous)

    # Push the document
    push.AddSingleDocument(mydoc)

    time.sleep(100)

    # Remove it
    push.RemoveSingleDocument('https://myreference&id=TESTME')


if __name__ == '__main__':
    main()
