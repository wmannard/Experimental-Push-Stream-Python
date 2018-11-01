# SDK Push API for Python
This SDK will help you to utilize the [Coveo Push API](https://docs.coveo.com/en/68/cloud-v2-developers/push-api) using Python.

## Description
The SDK makes it easier to communicate with the Coveo Push API. Documents are validated before being pushed. Updating documents will automatically call the update source status before and after uploading them. If files are to large, automatically an S3 large file container will be used.
All to make the life of the developer easier in adopting the Push API.

See the ```examples``` on how to use the SDK.

## How it works
Simply import the fowlling into your project:
```python
from coveopush import CoveoPush
from coveopush import Document
from coveopush import CoveoPermissions
from coveopush import CoveoConstants
```

## Installation
```
pip install coveopush
```

## Pushing documents
Coveo's Push API supports several methods of pushing data. Single calls and Batch calls. The recommended approach would be to use Batch calls. Using the SDK, you can use different approaches.
### Method A: Push a single document
Usage: When you simply need push a single document once in a while
NOT TO BE USED: When you need to update a lot of documents. Use Method C or Method B for that.
```python
push = CoveoPush.Push( sourceId, orgId, apiKey)
mydoc = CoveoDocument('https://myreference&id=TESTME')
mydoc.SetData( "ALL OF THESE WORDS ARE SEARCHABLE")
mydoc.FileExtension = ".html"
mydoc.AddMetadata("connectortype", "CSV")
mydoc.Title = "THIS IS A TEST"
user_email = "wim@coveo.com"
myperm = CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, "", user_email )
allowAnonymous = True
mydoc.SetAllowedAndDeniedPermissions([myperm], [], allowAnonymous)
push.AddSingleDocument(mydoc)
```

### Method B: Push a batch of documents in a single call
Usage: When you need to upload a lot of (smaller) documents
NOT TO BE USED: When you need to update a lot of LARGE documents. Use Method C for that.
```python
push = CoveoPush.Push( sourceId, orgId, apiKey)
batch=[]
batch.append(createDoc('testfiles\\BigExample.pdf'))
batch.append(createDoc('testfiles\\BigExample2.pptx'))
push.AddDocuments( batch, [], updateSourceStatus, deleteOlder)
```

### Method C: (RECOMMENDED APPROACH) Push a batch of documents, document by document
Usage: When you need to upload a lot of smaller/and or larger documents
NOT TO BE USED: When you have a single document. Use Method A for that.
```python
push = CoveoPush.Push( sourceId, orgId, apiKey)
push.Start( updateSourceStatus, deleteOlder)
push.SetSizeMaxRequest( 150*1024*1024 )
push.Add(createDoc('testfiles\\Large1.pptx','1'))
push.Add(createDoc('testfiles\\Large2.pptx','1'))
push.Add(createDoc('testfiles\\Large3.pptx','1'))
push.Add(createDoc('testfiles\\Large4.pptx','1'))
push.Add(createDoc('testfiles\\Large5.pptx','1'))
push.Add(createDoc('testfiles\\Large1.pptx','2'))
push.Add(createDoc('testfiles\\Large2.pptx','2'))
push.Add(createDoc('testfiles\\Large3.pptx','2'))
push.Add(createDoc('testfiles\\Large4.pptx','2'))
push.Add(createDoc('testfiles\\Large5.pptx','2'))
push.End( updateSourceStatus, deleteOlder)
```

## How to push a single document
You first create your Push source. And retrieve the Organization Id, Source Id and API Key. See [more info](https://docs.coveo.com/en/92/cloud-v2-developers/push-api-tutorial-1---managing-shared-content).

You now can initiate the CoveoPush:
```python
push = CoveoPush.Push( sourceId, orgId, apiKey)
```
To create a Document, suitable for the Push SDK:
```python
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
```
The above will create a document with an documentid of "https://myreference&id=TESTME". It will set the text for the document ```SetData```, some metadata and other properties like ```FileExtension``` and ```Title```.
Once the document is ready, you can push it to your index using:
```python
push.AddSingleDocument(mydoc)
```

This method works perfect for updates on single documents. If you have a larger set of documents to update, it is recommended to use the batch approach.


## How to push a batch of documents
When you need to push a lot of documents, use the batch approach.
The batch is first started with:
```python
push = CoveoPush.Push( sourceId, orgId, apiKey)
push.Start( updateSourceStatus, deleteOlder)
push.SetSizeMaxRequest( 150*1024*1024 )
```
The above will initialize the Push SDK. ```Start``` will start the batch operation. It will initialize the buffer and will make sure to update the source status to ```Rebuild```. The starting ordering id will be set, so that (later on) older documents can be removed.

Now you can add documents using:
```python
push.Add(createDoc('testfiles\\Large1.pptx','1'))
```
The ```Add``` will constantly check if the buffer does not exceed the maximum (256 Mb or the one you have set with ```SetSizeMaxRequest```). If so, it will initiate a Large File Upload to S3 and push the File Id to the Push API. The Push API will directly start processing the current batch.

Very important is to ```End``` the batch with:
```python
push.End( updateSourceStatus, deleteOlder)
```
This will flush the current buffer and push it to the index. It will update the source status back to ```Idle```, and delete older documents.

## How to add security to your documents
In a lot of cases you also want to add security to the documents you are pushing. A good [Tutorial can be found here](https://docs.coveo.com/en/98/cloud-v2-developers/push-api-tutorial-2---managing-secured-content).

The SDK supports security.
If you have your own security provider, you first need to create it with:
```python
#First set the securityprovidername
mysecprovidername = "MySecurityProviderTest"
#Define cascading security provider information
cascading = {
              "Email Security Provider": {
                "name": "Email Security Provider",
                "type": "EMAIL"
              }
            }
#Create it

push.AddSecurityProvider( mysecprovidername, "EXPANDED", cascading)
```
The ```AddSecurityProvider``` will automatically associate your current source with the newly created security provider.

Once the security provider is created, you can use it to set permissions on your documents.
A simple example:
```python
#Set permissions, based on an email address
user_email = "wim@coveo.com"
#Create a permission Identity
myperm = CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, "", user_email )
#Set the permissions on the document
allowAnonymous = False
mydoc.SetAllowedAndDeniedPermissions([myperm], [], allowAnonymous)
```

A more complicated example:
```python
#Set permissions
#Specific Users have permissions (in permLevel1/permLevel1Set) OR if you are in the group (permLevel2/permLevel2Set)
#This means: Two PermissionLevels
# Higher Permission Levels means priority
# So if you are allowed in Permission Level 1 and denied in Permission Level 2 you will still have access
#Denied users will always filtered out
#Users
users = []
users.append("wim")
users.append("peter")

#DeniedUsers
deniedusers = []
deniedusers.append("alex")
deniedusers.append("anne")

#Groups
groups = []
groups.append("HR")
groups.append("RD")
groups.append("SALES")

#Create the permission Levels, Each level can have multiple Sets
permLevel1 = CoveoPermissions.DocumentPermissionLevel('First')
permLevel1Set1 = CoveoPermissions.DocumentPermissionSet('1Set1')
permLevel1Set2 = CoveoPermissions.DocumentPermissionSet('1Set2')
permLevel1Set1.AllowAnonymous = False
permLevel1Set2.AllowAnonymous = False
permLevel2 = CoveoPermissions.DocumentPermissionLevel('Second')
permLevel2Set = CoveoPermissions.DocumentPermissionSet('2Set1')
permLevel2Set.AllowAnonymous = False

#Set the allowed for level1/set1
for user in users:
  #Create a permission Identity
  permLevel1Set1.AddAllowedPermission(CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, mysecprovidername, user ))
#Set the denied for level1/set2
for user in deniedusers:
  #Create a permission Identity
  permLevel1Set2.AddDeniedPermission(CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.User, mysecprovidername, user ))
#Set the allowed for level2/set1
for group in groups:
  #Create a permission Identity
  permLevel2Set.AddAllowedPermission(CoveoPermissions.PermissionIdentity( CoveoConstants.Constants.PermissionIdentityType.Group, mysecprovidername, group ))

#Set the Permissionsets to the Levels
permLevel1.AddPermissionSet( permLevel1Set1 )
permLevel1.AddPermissionSet( permLevel1Set2 )
permLevel2.AddPermissionSet( permLevel2Set )

#Set the permissions on the document
mydoc.Permissions.append( permLevel1 )
mydoc.Permissions.append( permLevel2 )
```
Permissions are created using Permission Levels, which hold multiple PermissionSets. More [Information](https://docs.coveo.com/en/25/cloud-v2-developers/complex-permission-model-definition-example).

Setting permissions with a custom security provider also requires that you inform the index which members and user mappings are available.
You do that normally after the indexing process is complete.

With securities there is also a batch call available.
You start the security expansions with:
```python
push.StartExpansion( mysecprovidername )
```
That will prepare the buffer.

Any group you have defined in your security must be properly expanded:
```python
for group in groups:
  #for each group set the users
  members = []
  for user in usersingroup:
    #Create a permission Identity
    members.append(CoveoPermissions.PermissionIdentityExpansion( CoveoConstants.Constants.PermissionIdentityType.User, mysecprovidername, user ))
  push.AddExpansionMember(CoveoPermissions.PermissionIdentityExpansion( CoveoConstants.Constants.PermissionIdentityType.Group, mysecprovidername, group ), members, [],[] )
```

And for each identiy you need to map it to the email security provider:
```python
for user in users:
  #Create a permission Identity
  mappings=[]
  mappings.append(CoveoPermissions.PermissionIdentityExpansion( CoveoConstants.Constants.PermissionIdentityType.User, "Email Security Provider", user+"@coveo.com" ))
  wellknowns=[]
  wellknowns.append(CoveoPermissions.PermissionIdentityExpansion( CoveoConstants.Constants.PermissionIdentityType.Group, mysecprovidername, "Everyone"))
  push.AddExpansionMapping(CoveoPermissions.PermissionIdentityExpansion( CoveoConstants.Constants.PermissionIdentityType.User, mysecprovidername, user ), [], mappings, wellknowns )
```

Same as the normal batch call, you also need to end the expansion with:
```python
push.EndExpansion( mysecprovidername )
```
That will flush the buffer, write it to S3 and push it to the Push API.
After the next ['Security Permission Update' cycle ](https://docs.coveo.com/en/1905/cloud-v2-administrators/security-identities---page#refresh-a-security-identity-provider), the securities will be updated.

### Dependencies
* [Python 3.x](https://www.python.org/downloads/)
* [Python Requests library]
* [Coveo Push Source](https://docs.coveo.com/en/92), Step 0 and Step 1



### References
* [Coveo Push API](https://docs.coveo.com/en/68/cloud-v2-developers/push-api)

### Authors
- Wim Nijmeijer (https://github.com/wnijmeijer)


