import xml.etree.ElementTree as ET
tree = ET.parse('ont_schema.xml')
root = tree.getroot()
tab_attr = {}
table_str = ''
# print(root.tag)
print(root.tag, ',', root.attrib['name'])
for child in root:
    print(child.tag, child.attrib)
    # print((child.attrib))
    print(table_str)
    table_str = ''
    for ctag in child:
        print('-----', ctag.tag, '-------')
        print(ctag.attrib)
        if ('alias' in ctag.attrib):
            table_str = table_str + ',' + ctag.attrib['alias']
        else:
            table_str

        if ('loadOrder' in ctag.attrib):
            table_str = table_str + ',' + ctag.attrib['loadOrder']
        else:
            table_str

        print(ctag.attrib['alias'], ctag.attrib['cached'], ctag.attrib['memory'], ctag.attrib['multiDataSource'], ctag.attrib['name'], ctag.attrib['rows'],
              ctag.attrib['snapshot'], ctag.attrib['source'], ctag.attrib['staging'], ctag.attrib['type'], ctag.attrib['displaySource'], ctag.attrib['loadOrder'])
        for tab in sorted(ctag.attrib.items()):
            try:
                print('Test')
                # print(tab['alias'], tab['cached'], tab['memory'], tab['multiDataSource'], tab['name'], tab['rows'],
                #      tab['snapshot'], tab['source'], tab['staging'], tab['type'], tab['displaySource'], tab['loadOrder'])
                # tab_attr[k] = 'Exists'
            except:
                print('Key Exists')
            # print(k)

# print(tab_attr)
