import xml.etree.ElementTree as ET
import glob
import os
import datetime
import db_connect
import read_mysql_db2
import read_business_schema
import table_attributes
schema_files = []
processed_files = []

instances = {'D': 'Development',
             'C': 'Certification',
             'P': 'Production'
             }
data_locs = {'extract_dir': '', 'processed': ''}
tab_attrs = table_attributes.tab_attrs


def main(file_name):
    # tree = ET.parse('./dev/schemas/test1.xml')
    # tree = ET.parse('ont_schema.xml')
    print('Processing file --> ', file_name)
    tree = ET.parse(file_name)
    root = tree.getroot()
    tab_attr = {}
    tab_attr_list = []
    col_attr_list = []
    table_str = ''
    schema_name = ''
    # print(root.tag)
    # print(root.tag, ',', root.attrib['name'])
    schema_name = root.attrib['name']
    for child in root:
        # print(child.tag, type(child))
        if child.tag == 'tables':
            process_tables(child, schema_name, file_name)
        if child.tag == 'joins':
            print_joins(child, schema_name)


def process_tables(table_data, sch_name, filename):
    tab_attr_list = []
    # print('From Process Tables --->')
    for ctag in table_data:
        if ctag.tag == 'table':
            for i in ctag.attrib.keys():
                if i not in tab_attr_list:
                    tab_attr_list.append(i)
    # tab_string1 = 'schemaName'
    # for t_head in sorted(tab_attr_list):
    #     tab_string1 = tab_string1 + ',' + t_head

    # table_file_name = sch_name + '_tables.csv'

    tab_string1 = 'schemaName,fileName'
    for t_head in (tab_attrs):
        tab_string1 = tab_string1 + ',' + t_head
    con_tab_file_name = instances[instances['S']] + '_tables.csv'
    if os.path.exists(os.getcwd() + '/processed/' + con_tab_file_name):
        # print('Table file exists, append to it')
        pass
    else:
        data_locs
        # with open(os.path.join('./processed', table_file_name), 'w') as f:
        with open(os.path.join('./processed', con_tab_file_name), 'w') as f:
            print(tab_string1, file=f)
    # columns_file_name = sch_name + '_columns.csv'
    con_col_file_name = instances[instances['S']] + '_columns.csv'
    if os.path.exists(os.getcwd() + '/processed/' + con_col_file_name):
        # print('Columns file exists, append to it')
        pass
    else:
        # with open(os.path.join('./processed', columns_file_name), 'w') as f:
        with open(os.path.join('./processed', con_col_file_name), 'w') as f:
            print(
                'schemaName,tableName,name,type,encrypt,function,hide,label,formula', file=f)
    for ctag in table_data:
        tab_str2 = sch_name + ',' + filename
        # with open(os.path.join('./processed', columns_file_name), 'a') as fc:
        with open(os.path.join('./processed', con_col_file_name), 'a') as fc:
            if ctag.tag == 'table':
                # print('In lop')
                for t_head in tab_attrs:  # (sorted(tab_attr_list)):
                    # print(t_head)
                    tab_str1 = ('NA' if ctag.attrib.get(
                        t_head) is None else ctag.attrib.get(t_head))
                    tab_str2 = tab_str2 + ',' + tab_str1
            for col_tag in ctag:
                print(sch_name, ',', ctag.attrib.get('name'), ',', col_tag.attrib.get('name'), ',', col_tag.attrib.get('type'), ',', col_tag.attrib.get(
                    'encrypt'), ',', col_tag.attrib.get('function'), ',', col_tag.attrib.get('hide'), ',', col_tag.attrib.get('label'), ',', 'NA' if col_tag.attrib.get('formula') is None else col_tag.attrib.get('formula'),  file=fc)
            # with open(os.path.join('./processed', table_file_name), 'a') as f:
            with open(os.path.join('./processed', con_tab_file_name), 'a') as f:
                print(tab_str2, file=f)


def print_joins(join_data, sch_name):
    # print('In Join Function')
    jname = ''
    # joins_file_name = sch_name + '_joins.csv'
    con_joins_file_name = instances[instances['S']] + '_joins.csv'
    if os.path.exists(os.getcwd() + '/processed/' + con_joins_file_name):
        # print('Joins file exists, append to it')
        pass
    else:
        # with open(os.path.join('./processed', joins_file_name), 'w') as f:
        with open(os.path.join('./processed', con_joins_file_name), 'w') as f:
            print('schemaName,joinName,joinElement,joinChild,joinOp,joinParent,filterField,filterOp,filterValue', file=f)
    for jtag in join_data:
        jname = (jtag.attrib.get('name'))
        for j in jtag:
            if j.tag == 'cond':  # Join Conditions
                with open(os.path.join('./processed', con_joins_file_name), 'a') as f:
                    print(sch_name, ',',  jname, ',', j.tag, ',', ('NA' if j.attrib.get(
                        'child') is None else j.attrib.get('child')), ',',
                        ('NA' if j.attrib.get(
                            'op') is None else j.attrib.get('op')), ',', ('NA' if j.attrib.get(
                                'parent') is None else j.attrib.get('parent')), ', , , ', file=f)
                # print(join_text)
                # print(type(join_text))
            if j.tag == 'filter':  # Join Filters
                for f in j:
                    with open(os.path.join('./processed', con_joins_file_name), 'a') as fil:
                        print(sch_name, ',', jname, ',', j.tag, ',', ', , ,', f.attrib.get('field'), ',',
                              f.attrib.get('op'), ',', f.attrib.get('value'), file=fil)


def check_files():
    print('-------')
    for f in schema_files[:]:
        print('File Name : ', f)
        processed_files.append(f)
        schema_files.remove(f)

    print('---After ----')
    for f in processed_files:
        print('File Name : ', f)
        # processed_files.append(f)
        # schema_files.remove(f)


def fetch_files(dir_loc):

    # print('Current Directory : ', os.getcwd())
    cwd = os.getcwd()
    print('Current Working Directory : ', cwd)
    os.chdir(dir_loc + '/schemas')
   # print('Current Directory : ', os.getcwd())
    for sch_files in glob.glob('*.xml'):
        print(sch_files)
        schema_files.append(sch_files)

    print('No. of files to Process : ', len(schema_files))
    go_or_no = input(
        'Do you want to process the files, type Y to Process or N for exit \n')

    if str.lower(go_or_no) == 'y':
        try:
            os.mkdir('processed')
        except:
            print(
                'Looks like processed directory already Exists, proceeding with next steps!')

        for file in schema_files:
            print(f'{len(processed_files)} of {len(schema_files)} files Processed ')
            try:
                main(file)
            except:
                print('Encountered errors in Processing filename : ', file)
            processed_files.append(file)
        print(f'{len(processed_files)} of {len(schema_files)} files Processed ')
    else:
        print('Exiting the Program..')


def get_inputs():
    print("--- Welcome to Incorta Metadata Extract and Program --- \n")
    print("Please Select which Incorta Instance to Connect")

    while 1:
        print(f"{str('Instance name').ljust(30)} : Code")
        print('----------------         ---------------')
        for k, v in instances.items():
            print(f'{str(v).ljust(30)} : {k}')
        instance_name = input("Please enter instance Code \n")
        if str(instance_name).upper() in ('D', 'DEV', 'C', 'CERT', 'P', 'PROD'):
            instance_name = str(instance_name[0]).upper()
            print('----------------------------------------')
            print(f'Connecting to : {instances[instance_name]} Instance')
            print('----------------------------------------')
            instances['S'] = instance_name
            break
        else:
            print(f"'{instance_name}' you provide is not a valid choice \n")
    data_dir = instance_name + '_' + \
        datetime.datetime.now().strftime("%d_%m_%Y")
    extract_loc = os.getcwd() + '/data/'+data_dir
    print(f"Default Extract directory: {extract_loc} \n")
    while 1:
        def_dir = input(
            'Do you want to change the directory ? enter Y for Yes and N for No  \n')
        if str(def_dir).strip().upper() in ('Y', 'YES'):
            dir_name = input('Enter folder name \n')
            if(os.path.isdir(os.getcwd()+'/data/' + dir_name)):
                print('Directory is found! Will extract data there')
                extract_loc = os.path.isdir(os.getcwd()+'/data/' + dir_name)
                break
            else:
                print(f'Folder {dir_name} not found, creating it')
                os.makedirs(os.getcwd()+'/data/' + dir_name)
                extract_loc = os.path.isdir(os.getcwd()+'/data/' + dir_name)
                break

        elif str(def_dir).strip().upper() in ('N', 'NO'):
            # if(os.path.isdir(os.getcwd()+'/data/' + data_dir)):
            if(os.path.isdir(extract_loc)):
                print('Default Directory is found! Will extract data there')
                break
            else:
                print(f'Folder {data_dir} not found, creating it')
                os.makedirs(extract_loc)
                break
        else:
            print('Not a valid option...')

    data_locs['extract_dir'] = extract_loc
    print('Data Dir : ', data_locs)

    """ while 1:
        extract_loc = input(
            "Please provide the full directory path to extract \n")
        if os.path.isdir(extract_loc):
            print('Directory Found, will process the below files...')
            # os.chdir(cwd+'/dev/schemas/')
            os.chdir(schema_loc)
            break
        else:
            print(
                f" '{schema_loc}' , is not a valid directory, please provide Valid directory!")
    print(f"Extract Location: \n {extract_loc}") """


if __name__ == "__main__":
    get_inputs()
    db_status = db_connect.connect_db(instances['S'])
    if db_status == 'S':
        print('DB Connection is Successful, Extracting the files...')
        print('PWD', os.getcwd())
        os.chdir(data_locs['extract_dir'])
        print('PWD', os.getcwd())
        read_mysql_db2.extract_data(data_locs['extract_dir'], instances['S'])
        read_business_schema.extract_data(
            data_locs['extract_dir'], instances['S'])
        print('PWD 2', os.getcwd())
        fetch_files(os.getcwd())

    # fetch_files()
    # check_files()

    # main()
