
# Code Review

## CoveoConstants

I don't understand why `class LargeFileContainer` is in there. It doesn't have constants.


## CoveoDocument

isBase64() is duplicated in CoveoPush. Maybe use a Util class?

in Validate(), rather than building a string for `error`, consider using an array and use `errors.append('some message')` and at the end, `' | '.join(errors)` (it will prevent trailings characters `|`)

### SetDate

The validation isn't right. It checks if not an empty string is passed when we expect a datetime object. I proposed we check if the instance is `datetime`, and in case a string is passed in, we try to make it a `datetime`.
For example:
```
    def SetDate(self, p_Date: datetime):
        """
        SetDate.
        Sets the date property.
        :arg p_Date: datetime, set the date
        """

        self.logger.debug('SetDate')
        # Check if string
        if (type(p_Date) is str):
            p_Date = datetime.fromisoformat(p_Date)

        # Check we have a datetime object
        if (type(p_Date) is not datetime):
            Error(self, "SetDate: invalid date")

        print('SetDate - trace 1 ' + p_Date.isoformat())
        self.Date = p_Date.isoformat()
```

## CoveoPermissions

We can reduce the number of functions we support.
It's logical to only use arrays to add members, mappings, etc...

We could remove these functions:
* DocumentPermissionSet::AddAllowedPermission
* DocumentPermissionSet::AddDeniedPermission
* PermissionIdentityBody::AddMember
* PermissionIdentityBody::AddMapping
* PermissionIdentityBody::AddWellKnown
* BatchPermissions::AddMember
* BatchPermissions::AddMapping
* BatchPermissions::AddDelete

If you want to keep them, I would defer their implementation to their array counterpart. This way, it would be easier to maintain.
For example:
```
  def AddMapping( self, p_PermissionIdentityBody: PermissionIdentityBody ):
    self.AddMappings( [p_PermissionIdentityBody] )
```

I updated the class `PermissionIdentityBody` to illustrate what I meant.

## CoveoPush

Do we really need all these functions: `GetDeleteDocumentUrl`, `GetDeleteOlderThanUrl`, `GetLargeFileContainerUrl`, `GetStatusUrl`, `GetUpdateDocumentUrl`, `GetUpdateDocumentsUrl`?

I feel that only a `GetUrl(path)` would be enough.

