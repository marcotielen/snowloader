import snowflake.connector


def open_connection(snow_envs, snow_user, snow_account, snow_authenticator=None
                    , snow_role=None, snow_warehouse=None, snow_database=None, snow_schema=None, snow_password=None,
                    snow_custom_account=None):

    if snow_account == 'Other':
        snow_account = snow_custom_account
    else:
        snow_account = next(item['account'] for item in snow_envs['instances'] if item["name"] == snow_account)

    if snow_authenticator == 'Externalbrowser (SSO)':
        snow_authenticator = 'externalbrowser'
    elif snow_authenticator == 'Snowflake':
        snow_authenticator = 'snowflake'
    # elif snow_authenticator is None or snow_authenticator == '':
    #     snow_authenticator = 'externalbrowser'

    if snow_warehouse == '':
        snow_warehouse is None # will use default
    if snow_role == '':
        snow_role is None # will use default

    con = snowflake.connector.connect(
        user=snow_user.upper(),
        account=snow_account,
        authenticator=snow_authenticator,
        role=snow_role,
        warehouse=snow_warehouse,
        database=snow_database,
        schema=snow_schema,
        password=snow_password
    )

    con_details = dict()
    con_details['user'] = snow_user.upper()
    con_details['account'] = snow_account
    con_details['authenticator'] = snow_authenticator
    con_details['role'] = snow_role
    con_details['warehouse'] = snow_warehouse
    con_details['database'] = snow_database
    # con_details['schema'] = snow_schema

    return con, con_details