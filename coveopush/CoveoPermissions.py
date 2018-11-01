#-------------------------------------------------------------------------------------
# CoveoPermissions
#-------------------------------------------------------------------------------------
# Contains the Permissions which are used inside the CoveoDocument
#   PermissionSets, PermisionLevels and Permissions
#-------------------------------------------------------------------------------------
from enum import Enum
from .CoveoConstants import Constants


#---------------------------------------------------------------------------------
def Error(log, err):
  raise Exception(err)
  
#---------------------------------------------------------------------------------
class PermissionIdentity:
  """
  class PermissionIdentity. 
  Class to hold the Permission Identity.
  identityType (User, Group, Virtual Group ==> PermissionIdentityType), 
  identity (for example: *@* or peter@coveo.com), 
  securityProvider (for example: Confluence Provider).
  """
  #The identityType (User, Group or Virtual Group).
  #PermissionIdentityType
  identityType=''

  #The associated identity provider identifier.
  #By default, if no securityProvider is specified, the identity will be associated the default
  #securityProvider defined in the configuration.
  securityProvider=''

  #The identity provided by the identity provider to identify the permission identity.
  identity=''

  #The additional information is a collection of key value pairs that
  #can be used to uniquely identify the permission identity.
  AdditionalInfo={}

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def __init__(self, p_IdentityType:Constants.PermissionIdentityType, p_SecurityProvider:str, p_Identity:str, p_AdditionalInfo: {}={} ):
    """
    class PermissionIdentity constructor. 
    :arg p_IdentityType: PermissionIdentityType.
    :arg p_SecurityProvider: Security Provider name
    :arg p_Identity: Identity to add
    :arg p_AdditionalInfo: AdditionalInfo dict {} to add
    """
    self.identity = p_Identity
    self.securityProvider = p_SecurityProvider
    self.identityType = p_IdentityType.value
    if not isinstance(p_AdditionalInfo,dict):
      raise Exception("PermissionIdentity: p_AdditionalInfo is not a dictionary")
    self.AdditionalInfo = p_AdditionalInfo

#---------------------------------------------------------------------------------
class PermissionIdentityExpansion:
  """
  class PermissionIdentityExpansion. 
  Class to hold the Permission Identity for expansion.
  identityType (User, Group, Virtual Group ==> PermissionIdentityType), 
  identity (for example: *@* or peter@coveo.com), 
  securityProvider (for example: Confluence Provider).
  """
  #The identityType/Type (User, Group or Virtual Group).
  #PermissionIdentityType
  type=''

  #The associated identity provider identifier.
  #By default, if no securityProvider is specified, the identity will be associated the default
  #securityProvider/Provider defined in the configuration.
  provider=''

  #The identity/name provided by the identity provider to identify the permission identity.
  name=''

  #The additional information is a collection of key value pairs that
  #can be used to uniquely identify the permission identity.
  additionalInfo={}

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def __init__(self, p_IdentityType:Constants.PermissionIdentityType, p_SecurityProvider:str, p_Identity:str, p_AdditionalInfo: {}={} ):
    """
    class PermissionIdentityExpansion constructor. 
    :arg p_IdentityType: PermissionIdentityType.
    :arg p_SecurityProvider: Security Provider name
    :arg p_Identity: Identity to add
    :arg p_AdditionalInfo: AdditionalInfo dict {} to add
    """
    self.name = p_Identity
    self.provider = p_SecurityProvider
    self.type = p_IdentityType.value
    if not isinstance(p_AdditionalInfo,dict):
      raise Exception("PermissionIdentityExpansion: p_AdditionalInfo is not a dictionary")
    self.additionalInfo = p_AdditionalInfo
#---------------------------------------------------------------------------------
class DocumentPermissionSet:
  """
  class DocumentPermissionSet. 
  Class to hold one Permission Set.
  """
  #The name of the permission set.
  Name=''

  #Whether to allow anonymous access to the document or not.
  AllowAnonymous=False

  #The allowed permissions. List of PermissionIdentity
  AllowedPermissions=[]

  #The denied permissions. List of PermissionIdentity
  DeniedPermissions=[]

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Default constructor used by the deserialization.
  def __init__(self, p_Name : str):
    self.Name = p_Name
    self.AllowAnonymous = False
    self.AllowedPermissions = []
    self.DeniedPermissions = []

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddAllowedPermission( self, p_PermissionIdentity: PermissionIdentity ):
    """
    AddAllowedPermission. 
    Add ONE PermissionIdentity to the AllowedPermissions
    :arg p_PermissionIdentity: PermissionIdentity.
    """
    #Check if correct
    if not (type(p_PermissionIdentity) is PermissionIdentity):
      Error(self, "AddAllowedPermission: value is not of type PermissionIdentity")
    
    self.AllowedPermissions.append(p_PermissionIdentity)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddDeniedPermission( self, p_PermissionIdentity: PermissionIdentity ):
    """
    AddDeniedPermission. 
    Add ONE PermissionIdentity to the DeniedPermissions
    :arg p_PermissionIdentity: PermissionIdentity.
    """
    #Check if correct
    if not (type(p_PermissionIdentity) is PermissionIdentity):
      Error(self, "AddDeniedPermission: value is not of type PermissionIdentity")
    
    self.DeniedPermissions.append(p_PermissionIdentity)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddAllowedPermissions( self, p_PermissionIdentities: [] ):
    """
    AddAllowedPermissions. 
    Add a list of PermissionIdentities to the AllowedPermissions
    :arg p_PermissionIdentities: list of PermissionIdentity.
    """
    #Check if correct
    if not p_PermissionIdentities:
      return
    if not isinstance(p_PermissionIdentities,(list,)):
      Error(self, "AddAllowedPermissions: value is not a list")

    if not (type(p_PermissionIdentities[0]) is PermissionIdentity):
      Error(self, "AddAllowedPermissions: value is not of type PermissionIdentity")
    
    self.AllowedPermissions.extend(p_PermissionIdentities)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddDeniedPermissions( self, p_PermissionIdentities: [] ):
    """
    AddDeniedPermissions. 
    Add a list of PermissionIdentities to the DeniedPermissions
    :arg p_PermissionIdentities: list of PermissionIdentity.
    """
    #Check if correct
    if not p_PermissionIdentities:
      return
    if not isinstance(p_PermissionIdentities,(list,)):
      Error(self, "AddDeniedPermissions: value is not a list")

    if not (type(p_PermissionIdentities[0]) is DocumentPermissionSet):
      Error(self, "AddDeniedPermissions: value is not of type PermissionIdentity")
    
    self.DeniedPermissions.extend(p_PermissionIdentities)


#---------------------------------------------------------------------------------
class DocumentPermissionLevel:
  """
  class DocumentPermissionLevel. 
  Class to hold one Permission Level.
  """
  #The name of the permission level.
  Name=''

  #The permission sets. Points to DocumentPermissionSet
  PermissionSets=[]

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Default constructor used by the deserialization.
  def __init__(self, p_Name: str):
    self.Name = p_Name
    self.PermissionSets = []

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddPermissionSet( self, p_DocumentPermissionSet: DocumentPermissionSet):
    """
    AddPermissionSet. 
    Add a DocumentPermissionSet to the current Level.
    :arg p_DocumentPermissionSet: DocumentPermissionSet.
    """
    #Check if correct
    if not (type(p_DocumentPermissionSet) is DocumentPermissionSet):
      Error(self, "AddPermissionSet: value is not of type DocumentPermissionSet")
    
    self.PermissionSets.append(p_DocumentPermissionSet)


#---------------------------------------------------------------------------------
class PermissionIdentityBody:
  """
  class PermissionIdentityBody. 
  Class to hold all associated Permission information for one Identity.
  """
  #The identity.
  #The identity is represented by a Name, a Type (User, Group or Virtual Group) and its Addtionnal Info).
  #PermissionIdentity
  identity=''

  #The mappings of a user.
  #Link different user identities in different systems that represent the same person.
  #For example:
  #     - corp\myuser (Active Directory)
  #     - myuser@myenterprise.com (Email)
  # List of PermissionIdentityExpansion
  mappings=[]

  #The members of a group or a virtual group (membership).
  # List of PermissionIdentityExpansion
  members=[]

  #The well-knowns.
  #Well-known is a group that identifies generic users or generic groups.
  #For example, in the Active Directory:
  # - Everyone: automatically includes everyone who uses the computer, even anonymous guests.
  # - Anonymous: automatically includes all users that have logged on anonymously.
  # List of PermissionIdentityExpansion
  wellKnowns=[]

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Default constructor used by the deserialization.
  def __init__(self, p_Identity: PermissionIdentityExpansion ):
    """
    Constructor PermissionIdentityBody. 
    :arg p_Identity: Identity name.
    """
    if not (type(p_Identity) is PermissionIdentityExpansion):
      Error(self, "PermissionIdentityBody constructor: value is not of type PermissionIdentityExpansion")
    self.identity = p_Identity
    self.mappings = []
    self.members = []
    self.wellKnowns = []
  

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMember( self, p_PermissionIdentity: PermissionIdentityExpansion ):
    """
    AddMember. 
    Add a PermissionIdentity to the Members.
    :arg p_PermissionIdentity: PermissionIdentityExpansion.
    """
    #Check if correct
    if not (type(p_PermissionIdentity) is PermissionIdentityExpansion):
      Error(self, "AddMember: value is not of type PermissionIdentityExpansion")
    
    self.members.append(p_PermissionIdentity)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMapping( self, p_PermissionIdentity: PermissionIdentityExpansion ):
    """
    AddMapping. 
    Add a PermissionIdentity to the Mappings.
    :arg p_PermissionIdentity: PermissionIdentityExpansion.
    """
    #Check if correct
    if not (type(p_PermissionIdentity) is PermissionIdentityExpansion):
      Error(self, "AddMapping: value is not of type PermissionIdentityExpansion")
    
    self.mappings.append(p_PermissionIdentity)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddWellKnown( self, p_PermissionIdentity: PermissionIdentityExpansion ):
    """
    AddWellKnown. 
    Add a PermissionIdentity to the WellKnowns.
    :arg p_PermissionIdentity: PermissionIdentity.
    """
    #Check if correct
    if not (type(p_PermissionIdentity) is PermissionIdentityExpansion):
      Error(self, "AddWellKnowns: value is not of type PermissionIdentityExpansion")
    
    self.wellKnowns.append(p_PermissionIdentity)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMembers( self, p_PermissionIdentities: [] ):
    """
    AddMembers. 
    Add a list of PermissionIdentities to the Members.
    :arg p_PermissionIdentities: list of PermissionIdentityExpansion.
    """
    #Check if correct
    if not p_PermissionIdentities:
      return
    if not isinstance(p_PermissionIdentities,(list,)):
      Error(self, "AddMembers: value is not a list")

    if not (type(p_PermissionIdentities[0]) is PermissionIdentityExpansion):
      Error(self, "AddMembers: value is not of type PermissionIdentityExpansion")
    
    self.members.extend(p_PermissionIdentities)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMappings( self, p_PermissionIdentities: [] ):
    """
    AddMappings. 
    Add a list of PermissionIdentities to the Mappings.
    :arg p_PermissionIdentities: list of PermissionIdentityExpansion.
    """
    #Check if correct
    if not p_PermissionIdentities:
      return
    if not isinstance(p_PermissionIdentities,(list,)):
      Error(self, "AddMappings: value is not a list")

    if not (type(p_PermissionIdentities[0]) is PermissionIdentityExpansion):
      Error(self, "AddMappings: value is not of type PermissionIdentityExpansion")
    
    self.mappings.extend(p_PermissionIdentities)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddWellKnowns( self, p_PermissionIdentities: [] ):
    """
    AddWellKnowns. 
    Add a list of PermissionIdentities to the WellKnowns.
    :arg p_PermissionIdentities: list of PermissionIdentityExpansion.
    """
    #Check if correct
    if not p_PermissionIdentities:
      return
    if not isinstance(p_PermissionIdentities,(list,)):
      Error(self, "AddWellKnowns: value is not a list")

    if not (type(p_PermissionIdentities[0]) is PermissionIdentityExpansion):
      Error(self, "AddWellKnowns: value is not of type PermissionIdentityExpansion")
    
    self.wellKnowns.extend(p_PermissionIdentities)

#---------------------------------------------------------------------------------
class BatchPermissions:
  """
  class BatchPermissions. 
  Class to hold the Batch Document.
  """
  # PermissionIdentityBody
  mappings = []
  # PermissionIdentityBody
  members = []
  # PermissionIdentityBody
  deleted = []

  #Default constructor used by the deserialization.
  def __init__(self):
    """
    Constructor BatchPermissions. 
    """
    self.mappings = []
    self.members = []
    self.deleted = []

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMember( self, p_PermissionIdentityBody: PermissionIdentityBody ):
    """
    AddMember. 
    Add a PermissionIdentityBody to the Members.
    :arg p_PermissionIdentityBody: PermissionIdentityBody.
    """
    #Check if correct
    if not (type(p_PermissionIdentityBody) is PermissionIdentityBody):
      Error(self, "AddMember: value is not of type PermissionIdentityBody")
    
    self.members.append(p_PermissionIdentityBody)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMapping( self, p_PermissionIdentityBody: PermissionIdentityBody ):
    """
    AddMapping. 
    Add a PermissionIdentityBody to the Mappings.
    :arg p_PermissionIdentityBody: PermissionIdentityBody.
    """
    #Check if correct
    if not (type(p_PermissionIdentityBody) is PermissionIdentityBody):
      Error(self, "AddMapping: value is not of type PermissionIdentity")
    
    self.mappings.append(p_PermissionIdentityBody)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddDelete( self, p_PermissionIdentityBody: PermissionIdentityBody ):
    """
    AddDelete. 
    Add a PermissionIdentityBody to the Delete.
    :arg p_PermissionIdentityBody: PermissionIdentityBody.
    """
    #Check if correct
    if not (type(p_PermissionIdentityBody) is PermissionIdentityBody):
      Error(self, "AddDelete: value is not of type PermissionIdentity")
    
    self.deleted.append(p_PermissionIdentityBody)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMembers( self, p_PermissionIdentityBodies: [] ):
    """
    AddMembers. 
    Add a list of p_PermissionIdentityBodies to the Members.
    :arg p_PermissionIdentityBodies: list of PermissionIdentityBody.
    """
    #Check if correct
    if not p_PermissionIdentityBodies:
      return
    if not isinstance(p_PermissionIdentityBodies,(list,)):
      Error(self, "AddMembers: value is not a list")

    if not (type(p_PermissionIdentityBodies[0]) is PermissionIdentityBody):
      Error(self, "AddMembers: value is not of type PermissionIdentity")
    
    self.members.extend(p_PermissionIdentityBodies)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddMappings( self, p_PermissionIdentityBodies: [] ):
    """
    AddMappings. 
    Add a list of p_PermissionIdentityBodies to the Mappings.
    :arg p_PermissionIdentities: list of PermissionIdentityBody.
    """
    #Check if correct
    if not p_PermissionIdentityBodies:
      return
    if not isinstance(p_PermissionIdentityBodies,(list,)):
      Error(self, "AddMappings: value is not a list")

    if not (type(p_PermissionIdentityBodies[0]) is PermissionIdentityBody):
      Error(self, "AddMappings: value is not of type PermissionIdentity")
    
    self.mappings.extend(p_PermissionIdentityBodies)

  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def AddDeletes( self, p_PermissionIdentityBodies: [] ):
    """
    AddWellKnowns. 
    Add a list of p_PermissionIdentityBodies to the Deletes.
    :arg p_PermissionIdentityBodies: list of PermissionIdentityBody.
    """
    #Check if correct
    if not p_PermissionIdentityBodies:
      return
    if not isinstance(p_PermissionIdentityBodies,(list,)):
      Error(self, "AddDeletes: value is not a list")

    if not (type(p_PermissionIdentityBodies[0]) is PermissionIdentityBody):
      Error(self, "AddDeletes: value is not of type PermissionIdentityBody")
    
    self.deleted.extend(p_PermissionIdentityBodies)

class SecurityProviderReference:
  id=''
  type='SOURCE'
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Default constructor used by the deserialization.
  def __init__(self, p_SourceId:str, p_type: str):
    """
    Constructor SecurityProviderReference. 
    :arg p_SourceId: Source id.
    :arg p_type: "SOURCE"
    """
    self.id = p_SourceId
    self.type = p_type

class SecurityProvider:
  name=''
  nodeRequired = False
  type=''
  referencedBy = []
  cascadingSecurityProviders = {}
