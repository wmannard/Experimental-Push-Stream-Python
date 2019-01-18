#!/usr/bin/env python
# -------------------------------------------------------------------------------------
# Push Single document
# -------------------------------------------------------------------------------------

import os

from coveopush import CoveoPush
from coveopush import Document


def main():
    sourceId = os.environ.get('PUSH_SOURCE_ID') or '--Enter your source id--'
    orgId = os.environ.get('PUSH_ORG_ID') or '--Enter your org id--'
    apiKey = os.environ.get('PUSH_API_KEY') or '--Enter your API key--'

    # Setup the push client
    push = CoveoPush.Push(sourceId, orgId, apiKey)

    # Create a document
    mydoc = Document("https://myreference/doc2")
    mydoc.SetData("This is document Two")
    mydoc.FileExtension = ".html"
    mydoc.AddMetadata("authors", "jdevost@coveo.com")
    mydoc.Title = "What's up Doc 2?"

    # Push the document
    push.AddSingleDocument(mydoc)


if __name__ == '__main__':
    main()
