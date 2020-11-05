import pandas as pd
df = pd.read_csv(
    '/Users/stammall/Sathish/Projects/Incorta_Metadata/data/P_27_10_2020/schemas/processed/Production_tables.csv')
df_cert = pd.read_csv(
    '/Users/stammall/Sathish/Projects/Incorta_Metadata/data/D_27_10_2020/schemas/processed/Development_tables.csv')
print('Schema Dev Tables  : ',  df['name'].count())
print('Cert Tables : ', df_cert['name'].count())

tab_diff = pd.concat([df[['name', 'schemaName']], df_cert[[
                     'name', 'schemaName']]]).drop_duplicates(keep=False)

print(tab_diff.count())

schema_diff = pd.concat([df[['schemaName']], df_cert[[
    'schemaName']]]).drop_duplicates(keep=False)

print('Schema Diff : ', schema_diff.count())

print(df['schemaName'].count())
print(len(df.schemaName.unique()))

# left = pd.DataFrame({'key1': ['K0', 'K0', 'K1', 'K2'], 'key2': [
#                     'K0', 'K1', 'K0', 'K1'], 'A': ['A0', 'A1', 'A2', 'A3'], 'B': ['B0', 'B1', 'B2', 'B3']})

# right = pd.DataFrame({'key1': ['K0', 'K1', 'K1', 'K2'], 'key2': [
#                      'K0', 'K0', 'K0', 'K0'], 'C': ['C0', 'C1', 'C2', 'C3'], 'D': ['D0', 'D1', 'D2', 'D3']})

# result = pd.merge(left, right, how='left', on=['key1', 'key2'])

# df['Schema'] = 'ONT'

df = df.assign(instance='PROD')
df_cert = df_cert.assign(instance='CERT')
# df_cert['Schema'] = 'ONT'

# print(df[['schema', 'name', 'instance']])

left = df[['schema', 'name', 'instance']]
right = df_cert[['schema', 'name', 'instance']]


# result = pd.merge(left, right, how='outer', on=['schema', 'name'])


# print(result)
