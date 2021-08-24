import boto3
import pandas as pd

lfc = boto3.client('lakeformation')
this_aws_account = boto3.client('sts').get_caller_identity().get('Account')

dl_admins = lfc.get_data_lake_settings()

df_lf_defaults = pd.DataFrame(
    {
        'CreateDatabaseDefaultPermissions': dl_admins['DataLakeSettings']['CreateDatabaseDefaultPermissions'],
        'CreateTableDefaultPermissions': dl_admins['DataLakeSettings']['CreateTableDefaultPermissions'],
        'TrustedResourceOwners': dl_admins['DataLakeSettings']['TrustedResourceOwners']
    }
)

account = []
principal_type = []
principal_name = []
principal_arn = []
for principal in dl_admins['DataLakeSettings']['DataLakeAdmins']:
    this_account, this_principal = (principal['DataLakePrincipalIdentifier'].split(':')[4:6])
    account.append(this_account)
    this_principal_type, this_principal_name = this_principal.split('/')
    principal_type.append(this_principal_type)
    principal_name.append(this_principal_name)
    principal_arn.append(principal['DataLakePrincipalIdentifier'])

df_lf_admins = pd.DataFrame(
    {
        'AWS Account': account,
        'Principal Type': principal_type,
        'Principal Name': principal_name,
        'Principal ARN': principal_arn
    }
)

all_dl_permissions = []
dl_permissions = lfc.list_permissions(MaxResults=1000)  # len(dl_permissions['PrincipalResourcePermissions'])
all_dl_permissions[len(all_dl_permissions):] = dl_permissions['PrincipalResourcePermissions']
while dl_permissions.get('NextToken', False):
    dl_permissions = lfc.list_permissions(MaxResults=1000, NextToken=dl_permissions['NextToken'])
    all_dl_permissions[len(all_dl_permissions):] = dl_permissions['PrincipalResourcePermissions']

'''
import json
print(json.dumps(all_dl_permissions, indent=4))
'''

account = []
principal_type = []
principal_name = []
principal_arn = []

catalog_id = []
database_name = []
table_name = []
table_wild_card = []
column_names = []
column_names_excluded = []
data_location_arn = []

tag_key = []
tag_values = []

tag_policy_type = []
tag_policy_key = []
tag_policy_values = []

permissions = []
permissions_with_grant_option = []
resource_share = []

for principal_permissions in all_dl_permissions:

    this_split = principal_permissions['Principal']['DataLakePrincipalIdentifier'].split(':')
    if len(this_split) == 1:
        this_account = this_aws_account
        this_principal = this_split[0]
    else:
        this_account, this_principal = (principal_permissions['Principal']['DataLakePrincipalIdentifier'].split(':')[4:6])
    account.append(this_account)
    this_split = this_principal.split('/')
    this_principal_type, this_principal_name = ('/'.join(this_split[0:-1]), this_split[-1])
    principal_type.append(this_principal_type)
    principal_name.append(this_principal_name)
    principal_arn.append(principal_permissions['Principal']['DataLakePrincipalIdentifier'])

    this_catalog_id = None  # noqa
    this_database_name = None  # noqa
    this_table_name = None  # noqa
    this_table_wild_card = None  # noqa
    this_table_name = None  # noqa
    this_column_names = None  # noqa
    this_column_names_excluded = None  # noqa
    this_data_location_arn = None  # noqa

    this_tag_key = None  # noqa
    this_tag_values = None  # noqa

    this_tag_policy_type = None  # noqa
    this_tag_policy_key = None  # noqa
    this_tag_policy_values = None  # noqa

    this_permissions = None  # noqa
    this_permissions_with_grant_option = None  # noqa
    this_resource_share = None  # noqa

    resources_list = list(principal_permissions['Resource'].keys())
    if 'Database' in resources_list:
        this_catalog_id = principal_permissions['Resource']['Database'].get('CatalogId')
        this_database_name = principal_permissions['Resource']['Database'].get('Name')
    if 'Table' in resources_list:
        this_catalog_id = principal_permissions['Resource']['Table'].get('CatalogId')
        this_database_name = principal_permissions['Resource']['Table'].get('DatabaseName')
        this_table_name = principal_permissions['Resource']['Table'].get('Name')
        this_table_wild_card = principal_permissions['Resource']['Table'].get('TableWildcard')
    if 'TableWithColumns' in resources_list:
        this_catalog_id = principal_permissions['Resource']['TableWithColumns'].get('CatalogId')
        this_database_name = principal_permissions['Resource']['TableWithColumns'].get('DatabaseName')
        this_table_name = principal_permissions['Resource']['TableWithColumns'].get('Name')
        this_column_names = principal_permissions['Resource']['TableWithColumns'].get('ColumnNames')
        this_column_names = None if this_column_names is None else '|'.join(this_column_names)
        this_column_names_excluded = principal_permissions['Resource']['TableWithColumns']['ColumnWildcard'].get('ExcludedColumnNames')
        this_column_names_excluded = None if this_column_names_excluded is None else '|'.join(this_column_names_excluded)
    if 'DataLocation' in resources_list:
        this_catalog_id = principal_permissions['Resource']['DataLocation'].get('CatalogId')
        this_data_location_arn = principal_permissions['Resource']['DataLocation'].get('ResourceArn')
    if 'LFTag' in resources_list:
        this_catalog_id = principal_permissions['Resource']['LFTag'].get('CatalogId')
        this_tag_key = principal_permissions['Resource']['LFTag'].get('TagKey')
        this_tag_values = principal_permissions['Resource']['LFTag'].get('TagValues')
        this_tag_values = None if this_tag_values is None else '|'.join(this_tag_values)
    if 'LFTagPolicy' in resources_list:
        this_catalog_id = principal_permissions['Resource']['LFTagPolicy'].get('CatalogId')
        this_tag_policy_type = principal_permissions['Resource']['LFTagPolicy'].get('ResourceType')
        this_tag_policy_key = principal_permissions['Resource']['LFTagPolicy']['Expression'].get('TagKey')
        this_tag_policy_values = principal_permissions['Resource']['LFTagPolicy']['Expression'].get('TagValues')
        this_tag_policy_values = None if this_tag_policy_values is None else '|'.join(this_tag_policy_values)
    this_permissions = principal_permissions.get('Permissions')
    this_permissions = None if this_permissions is None else '|'.join(this_permissions)
    this_permissions_with_grant_option = principal_permissions.get('PermissionsWithGrantOption')
    this_permissions_with_grant_option = None if this_permissions_with_grant_option is None else '|'.join(
        this_permissions_with_grant_option)
    this_resource_share = principal_permissions.get('ResourceShare')
    this_resource_share = None if this_resource_share is None else '|'.join(this_resource_share)

    catalog_id.append(this_catalog_id)
    database_name.append(this_database_name)
    table_name.append(this_table_name)
    table_wild_card.append(this_table_wild_card)
    column_names.append(this_column_names)
    column_names_excluded.append(this_column_names_excluded)
    data_location_arn.append(this_data_location_arn)

    tag_key.append(this_tag_key)
    tag_values.append(this_tag_values)

    tag_policy_type.append(this_tag_policy_type)
    tag_policy_key.append(this_tag_policy_key)
    tag_policy_values.append(this_tag_policy_values)

    permissions.append(this_permissions)
    permissions_with_grant_option.append(this_permissions_with_grant_option)
    resource_share.append(this_resource_share)

df_lf_permissions = pd.DataFrame(
    {
        'Account': account,
        'Catalog ID': catalog_id,
        'Principal Type': principal_type,
        'Principal Name': principal_name,
        'Principal ARN': principal_arn,

        'Database Name': database_name,
        'Table Name': table_name,
        'Table Wild card': table_wild_card,
        'Column Names': column_names,
        'Excluded Column Names': column_names_excluded,
        'DataLocation ARN': data_location_arn,

        'Tag Key': tag_key,
        'Tag Values': tag_values,

        'Tag ResourceType': tag_policy_type,
        'Tag Policy Key': tag_policy_key,
        'Tag Policy Values': tag_policy_values,

        'Permissions': permissions,
        'Permissions With Grant Option': permissions_with_grant_option,
        'Resource Share': resource_share
    }
)

with pd.ExcelWriter(f'LF_Permissions_Audit_{this_aws_account}.xlsx') as writer:
    df_lf_defaults.to_excel(writer, sheet_name="defaults", index=False)
    df_lf_admins.to_excel(writer, sheet_name="admins", index=False)
    df_lf_permissions.to_excel(writer, sheet_name="permissions", index=False)
