#!/usr/bin/env python
# -------------------------------------------------------------------------------------
# Push Multiple From CSV using BATCH calls
# -------------------------------------------------------------------------------------

import csv
import datetime
import os

from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush import CoveoConstants

# Add a document


def add_document(post):
    # Create new push document
    mydoc = Document('https://myreference&id='+post['UserName'])
    # Build up the quickview/preview (HTML)
    content = "<meta charset='UTF-16'><meta http-equiv='Content-Type' content='text/html; charset=UTF-16'><html><head><title>"+post['FirstName']+" "+post['LastName']+" ("+post['JobFunction']+")</title><style>.datagrid table { border-collapse: collapse; text-align: left; } .datagrid {display:table !important;font: normal 12px/150% Arial, Helvetica, sans-serif; background: #fff; overflow: hidden; border: 1px solid #006699; -webkit-border-radius: 3px; -moz-border-radius: 3px; border-radius: 3px; }.datagrid table td, .datagrid table th { padding: 3px 10px; }.datagrid table thead th {background:-webkit-gradient( linear, left top, left bottom, color-stop(0.05, #006699), color-stop(1, #00557F) );background:-moz-linear-gradient( center top, #006699 5%, #00557F 100% );filter:progid:DXImageTransform.Microsoft.gradient(startColorstr='#006699', endColorstr='#00557F');background-color:#006699; color:#FFFFFF; font-size: 15px; font-weight: bold; border-left: 1px solid #0070A8; } .datagrid table thead th:first-child { border: none; }.datagrid table tbody td { color: #00496B; border-left: 1px solid #E1EEF4;font-size: 12px;font-weight: normal; }.datagrid table tbody  tr:nth-child(even)  td { background: #E1EEF4; color: #00496B; }.datagrid table tbody td:first-child { border-left: none; }.datagrid table tbody tr:last-child td { border-bottom: none; }</style></head><body style='Font-family:Arial'><div class='datagrid'><table><tbody><tr><td>FirstName</td><td>"+post[
        'FirstName']+"</td></tr><tr><td>MiddleName</td><td>"+post['MiddleName']+"</td></tr><tr><td>LastName</td><td>"+post['LastName']+"</td></tr><tr><td>PositionDescription</td><td>"+post['PositionDescription']+"</td></tr><tr><td>JobFunction</td><td>"+post['JobFunction']+"</td></tr><tr><td>JobFamily</td><td>post['JobFamily']</td></tr></tbody></table></div></body></html>"
    mydoc.SetContentAndZLibCompress(content)

    # Set the fileextension
    mydoc.FileExtension = ".html"
    # Set metadata
    mydoc.AddMetadata("connectortype", "CSV")
    authors = []
    authors.append("Coveo")
    authors.append("R&D")
    # rssauthors should be set as a multi-value field in your Coveo Cloud organization
    mydoc.AddMetadata("rssauthors", authors)
    # Set the date
    mydoc.SetDate(datetime.datetime.now())
    mydoc.SetModifiedDate(datetime.datetime.now())
    mydoc.Title = post['FirstName']+' ' + post['LastName']+' '+'('+post['JobFunction']+')'

    # Set permissions
    user_email = "wim@coveo.com"
    myperm = CoveoPermissions.PermissionIdentity(CoveoConstants.Constants.PermissionIdentityType.User, "", user_email)
    mydoc.SetAllowedAndDeniedPermissions([myperm], [], True)

    print('\nUser %s for title "%s"' % (user_email, post['FirstName']))
    return mydoc


def main():
    # setup Push client
    sourceId = os.environ.get('PUSH_SOURCE_ID') or '--Enter your source id--'
    orgId = os.environ.get('PUSH_ORG_ID') or '--Enter your org id--'
    apiKey = os.environ.get('PUSH_API_KEY') or '--Enter your API key--'

    updateSourceStatus = True
    deleteOlder = True

    # Create the push client
    push = CoveoPush.Push(sourceId, orgId, apiKey, p_Mode=CoveoConstants.Constants.Mode.UpdateStream)

    push.Start(updateSourceStatus, deleteOlder)

    # Create a batch of documents
    batch = []
    myfile = os.path.join('testfiles', 'People.csv')
    with open(myfile, 'r') as infile:
        posts = csv.DictReader(infile, delimiter=';')
        # loop through each post and add to Coveo
        for row in posts:
            myCoveoDocument = add_document(row)
            push.Add(myCoveoDocument)

    # End the Push
    push.End(updateSourceStatus, deleteOlder)


if __name__ == '__main__':
    main()
