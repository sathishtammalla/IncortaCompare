import datetime
import mysql.connector
import sqlalchemy as db
import os
import glob
import errno
   
# specify database configurations
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'sattin3010',
    'database': 'incorta_cluster',
    'raise_on_warnings': True
}
db_user = config.get('user')
db_pwd = config.get('password')
db_host = config.get('host')
db_port = config.get('port')
db_name = config.get('database')
# specify connection string
connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
# connect to database
engine = db.create_engine(connection_str)
connection = engine.connect()
# pull metadata of a table
metadata = db.MetaData(bind=engine)

# Map the table to Object
data = db.Table('SCHEMA', metadata,
                autoload=True, autoload_with=engine)
# Select the required columns
query = db.select(
    [data.c.ID, data.c.NAME, data.c.SCHEMADATA, data.c.LOADERDATA])
# query = db.select([data.c.ID, data.c.NAME, data.c.SCHEMADATA]).where(
#     data.c.NAME == 'EBS_ONT')
print(query)
# results = connection.execute(query, NAME_1='EBS_ONT')
results = connection.execute(query)
row = results.fetchone()
print(results.rowcount)
# print(f'Name: {row["NAME"]}, Data : {row["SCHEMADATA"]}')
print('Extraction Start Time: ', datetime.datetime.now())
start_time = datetime.datetime.now()
os.chdir(os.getcwd()+'/dev/extract/')

dir_name = os.getcwd()+'/schemas1'
print(dir_name)

print(os.path.exists(os.path.dirname(dir_name)))

if not os.path.exists(dir_name):
    print('In directory Check')
    try:
        os.makedirs('schemas1')
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

for res in results:
    # print(res['NAME'], res['SCHEMADATA'].decode(encoding='UTF-8'))
    # print(res['NAME'])
    with open(os.path.join('./schemas1', str(res['ID']) + '_' + res['NAME']+'.xml'), 'w') as f:
        print((res['SCHEMADATA'].decode(encoding='UTF-8')), file=f)

    if res['LOADERDATA'] is None:
        pass
    else:
        with open(os.path.join('./loader', str(res['ID']) + '_' + res['NAME']+'.xml'), 'w') as l:
            print((res['LOADERDATA'].decode(encoding='UTF-8')), file=l)
    """ if(res['NAME'] == 'EBS_AP'):
        # print(res['ID'], res['NAME'])
        print('Current Directory: ', os.getcwd())
        os.chdir(os.getcwd()+'/dev/extract/')
        print('Changed Directory: ', os.getcwd())
        print('ONT Data')
        with open(res['NAME']+'.xml', 'w') as f:
            # print('schemaName,joinName,joinElement,joinChild,joinOp,joinParent,filterField,filterOp,filterValue', file=f)
            print((res['SCHEMADATA'].decode(encoding='UTF-8')), file=f) """
print('Extraction End Time: ', datetime.datetime.now())
end_time = datetime.datetime.now()
print('Total Extract Time: ', end_time - start_time)
