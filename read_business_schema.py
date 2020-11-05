import datetime
import mysql.connector
import sqlalchemy as db
import os
import glob
import errno
import db_config


def extract_data(dir_loc, instance):
    # specify database configurations
    """    config = {
           'host': 'localhost',
           'port': 3306,
           'user': 'root',
           'password': 'sattin3010',
           'database': 'incorta_cluster',
           'raise_on_warnings': True
       } """

    config = db_config.config[instance]
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
        [data.c.ID, data.c.NAME, data.c.SCHEMADATA, data.c.LOADERDATA]).where(data.c.SCHEMATYPE == 1)
    # query = db.select([data.c.ID, data.c.NAME, data.c.SCHEMADATA]).where(
    #     data.c.NAME == 'EBS_ONT')
    print(query)
    # results = connection.execute(query, NAME_1='EBS_ONT')
    results = connection.execute(query)
    #row = results.fetchone()
    print(results.rowcount)
    # print(f'Name: {row["NAME"]}, Data : {row["SCHEMADATA"]}')
    print('Extraction Start Time: ', datetime.datetime.now())
    start_time = datetime.datetime.now()
    os.chdir(dir_loc)
    # os.chdir(os.getcwd()+'/dev/extract/')

    dir_name = os.getcwd()+'/bschemas'
    print(dir_name)

    print(os.path.exists(os.path.dirname(dir_name)))

    if not os.path.exists(dir_name):
        print('In directory Check')
        try:
            os.makedirs('bschemas')
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    for res in results:
        # print(res['NAME'], res['SCHEMADATA'].decode(encoding='UTF-8'))
        # print(res['NAME'])
        try:
            with open(os.path.join('./bschemas', str(res['ID']) + '_' + res['NAME']+'.xml'), 'w') as f:
                print((res['SCHEMADATA'].decode(encoding='UTF-8')), file=f)
        except:
            print('Error in writing file ', str(
                res['ID']) + '_' + res['NAME']+'.xml')

        """ if res['LOADERDATA'] is None:
            pass
        else:
            with open(os.path.join('./loader', str(res['ID']) + '_' + res['NAME']+'.xml'), 'w') as l:
                print((res['LOADERDATA'].decode(encoding='UTF-8')), file=l) """

    print('Extraction End Time: ', datetime.datetime.now())
    end_time = datetime.datetime.now()
    print('Total Extract Time: ', end_time - start_time)
    return 'S'
