from minio import Minio
from configs.host import host

credential_postgres_adventureworks = {
    'host': host,
    'dbname': 'Adventureworks',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5435
}

credential_minio = Minio(
    endpoint=f'{host}:9000',
    access_key='chapolin',
    secret_key='mudar@123',
    secure=False
)

local_path = 'local_data/'
    
tables_postgres_adventureworks = {
    '1': 'sales.countryregioncurrency',
    #'2': 'sales.creditcard',
    #'3': 'sales.currency',
    #'4': 'sales.currencyrate',
    #'5': 'sales.customer',
    #'6': 'sales.personcreditcard',
    #'7': 'sales.salesorderdetail',
    #'8': 'sales.salestaxrate',  
    #'9': 'sales.salesorderheadersalesreason',
    #'10': 'sales.salesperson',
    #'11': 'sales.salespersonquotahistory',
    #'12': 'sales.salesreason',
    #'13': 'sales.salestaxrate',
    #'14': 'sales.salesterritory',
    #'15': 'sales.salesterritoryhistory',
    #'16': 'sales.shoppingcartitem',
    #'17': 'sales.specialoffer',
    #'18': 'sales.specialofferproduct',
    #'19': 'sales.store',
    '20': 'humanresources.department',
    '21': 'humanresources.employee',
    '22': 'sales.salesorderheader'    
}
