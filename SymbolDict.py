"""
Data description:
                AMap_adcode.csv is 高德行政区划+sdcode
                shortname_adcode.csv is 省级简称+adcode
                city_alias.csv is 城市别称
                minority.csv is 少数民族列表
Function:
                从AMap_adcode.csv提取极简化的地名符号
                并与简称和别称的地名符合合并，构建地名符号与地名编号的映射字典
"""
import pandas as pd
#读取AMap_adcode.csv, addname_adcode.csv, minority.csv为df_mainname,df_aliasname和list_minority
df = pd.read_csv('data/input/AMap_adcode.csv')
df_alias = pd.read_csv('data/input/city_alias.csv')
df_shortname_code = pd.read_csv('data/input/shortname_adcode copy.csv')
minority_names_df = pd.read_csv('data/input/minority.csv',header=None)
# Convert the minority names into a set for faster lookup.
minority_names = set(minority_names_df.iloc[:, 0].values)

# Function to process city names.
def minimalize_city_name(city_name):
    # Remove minority chracters in city names.
    for minority in minority_names:
        city_name = city_name.replace(minority, '')
    
    # Remove specific substrings,like'自治','特别'.
    city_name = city_name.replace('自治', '').replace('特别', '').replace('行政', '').replace('直辖', '')
    city_name = city_name.replace('左翼前', '').replace('左翼中', '').replace('左翼后', '').replace('右翼前', '').replace('右翼中', '').replace('右翼后', '').replace('左翼', '')
    # Remove the last character if the length is greater than 2, the administrative unit character is removed
    if len(city_name) > 2:
        city_name = city_name[:-1]

    return city_name
"""
对df的操作
"""
#删除地名中包含“市辖区”的行
df = df[~df['fullname'].str.contains("市辖区")]
#对地名极简化处理
df['fullname'] = df['fullname'].apply(minimalize_city_name)
#对极简化后的重复地名处理
# 找出 'fullname' 列重复的所有行
duplicates = df[df['fullname'].duplicated(keep=False)]
# 对于重复的 'fullname'，保留 'adcode' 末尾包含两个 0 或四个 0 的行,当简化后的地名重复时，删除小地名，保留大地名
df_filtered = duplicates[duplicates['adcode'].astype(str).str.endswith('00') | 
                         duplicates['adcode'].astype(str).str.endswith('0000')]
# 从原始 df 中删除不满足条件的重复行得到df_mainname_code
df_mainname_code = pd.concat([df.drop(duplicates.index), df_filtered], ignore_index=True)
df_mainname_code = df_mainname_code.rename(columns={'fullname': 'mainname'})
"""
对df_alias的操作
"""
#将df_alias与 df_mainname_code 连接
df_alias_mergedcode = pd.merge(df_alias, df_mainname_code, on='mainname', how='left')
df_alias_code = df_alias_mergedcode[['alias','adcode']]
print(df_alias_code)
#纵向合并df_mainname_code,df_alias_code,df_shortname_code为df_placesymbol_code,需要修改为一致的列名
df_mainname_code.rename(columns={'mainname': 'placesymbol', 'adcode': 'placecode'}, inplace=True)
df_alias_code.rename(columns={'alias': 'placesymbol', 'adcode': 'placecode'}, inplace=True)
df_shortname_code.rename(columns={'shortname': 'placesymbol', 'adcode': 'placecode'}, inplace=True)
# 使用 concat 方法纵向合并这三个 DataFrame，并直接对合并后的 DataFrame 按照 'code' 升序排列，重置行索引
df_placesymbol_code = pd.concat([df_mainname_code, df_alias_code, df_shortname_code], ignore_index=True).sort_values(by='placecode').reset_index(drop=True)
#输出地名符号与地名编码
df_placesymbol_code.to_csv('data/output/placesymbol_code.csv',index=0)

# # 将 DataFrame 转换为字典，以 'code' 列作为键，'name' 列作为值
# dictionary = df_placesymbol_code.set_index('name')['code'].to_dict()
