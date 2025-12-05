import pandas as pd
from etl.utils.connect_lakehouse import get_lakehouse_connection


def extract_banner_customers():
    """
    Extract Banner customer master data from the Fabric Lakehouse SQL endpoint.
    Returns a Pandas DataFrame exactly as the source provides it (no renaming).
    """

    query = """
        SELECT 
            dataAreaId,
            CustomerAccount,
            PrimaryContactEmail,
            PrimaryContactPhone,
            OrganizationName,
            CreditLimit,
            SiteId,
            NameAlias,
            CustomerGroupId,
            CredManAccountStatusId,
            SalesSegmentId,

            -- Delivery Address Fields
            DeliveryAddressDescription,
            DeliveryAddressStreet,
            DeliveryAddressCity,
            DeliveryAddressCounty,
            DeliveryAddressState,
            DeliveryAddressZipCode,
            DeliveryAddressCountryRegionId,

            -- Primary Address Fields
            AddressDescription,
            FullPrimaryAddress,
            AddressStreet,
            AddressCity,
            AddressCounty,
            AddressState,
            AddressZipCode,
            AddressCountryRegionId

        FROM dbo.Customers_all;
    """

    with get_lakehouse_connection() as conn:
        df = pd.read_sql(query, conn)

    return df



