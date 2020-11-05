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


def main():
    tree = ET.parse('./data/P_27_10_2020/bschemas/460_Holiday_Project.xml')
    # tree = ET.parse('ont_schema.xml')
    #print('Processing file --> ', file_name)
    #tree = ET.parse(file_name)
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
        print(child.tag, type(child))
        if child.tag == 'tables':
            #process_tables(child, schema_name, file_name)
            for ctag in child:
                if ctag.tag == 'table':
                    for i in ctag.attrib.keys():
                        print('Table attrib', i)
        if child.tag == 'businessViews':
            #print_joins(child, schema_name)
            print_business_views(child, schema_name)
            """ print('schemaName,viewName,baseTable,viewOrder')
            for vtag in child:
                if vtag.tag == 'businessView':
                    for col in vtag:
                        print(vtag.attrib.get('name'), col.attrib.get('name'), col.attrib.get('label'), ('NA' if col.attrib.get(
                            'source') is None else col.attrib.get('source')),
                            ('NA' if col.attrib.get(
                                'formula') is None else col.attrib.get('formula')), ('NA' if col.attrib.get(
                                    'function') is None else col.attrib.get('function')))
                print(schema_name, vtag.attrib.get('name'), ('NA' if vtag.attrib.get(
                    'baseTable') is None else vtag.attrib.get('baseTable')), vtag.attrib.get('viewOrder')) """

    print('Current Directory :', os.getcwd())


def print_business_views(bview, schema_name):
    print('schemaName,viewName,baseTable,viewOrder')
    for vtag in bview:
        if vtag.tag == 'businessView':
            for col in vtag:
                print(vtag.attrib.get('name'), col.attrib.get('name'), col.attrib.get('label'), ('NA' if col.attrib.get(
                    'source') is None else col.attrib.get('source')),
                    ('NA' if col.attrib.get(
                        'formula') is None else col.attrib.get('formula')), ('NA' if col.attrib.get(
                            'function') is None else col.attrib.get('function')))
        print(schema_name, vtag.attrib.get('name'), ('NA' if vtag.attrib.get(
            'baseTable') is None else vtag.attrib.get('baseTable')), vtag.attrib.get('viewOrder'))


if __name__ == "__main__":
    main()
