"""
Data description:
                placesymbol_code.csv is 地名符号与地名编号的映射字典, fields are 'placesymbol', 'placecode'.
                csv_directory is documentary of POI files
                Fields in each POI file are: name,_id,adcode,placesymbol,placecode
Function:
                遍历读取指定文件夹下所有csv
                遍历读取每个csv为一个单独的dataframe
                修改 adcode 和 placecode 的值：首先将它们截取为前4位。然后，如果这些值是1100、1200、3100、5000中的一个，分别将它们修改为1101、1201、3101、5001。
                进行条件判断并删除行：删除 adcode 和 placecode 值相同的行。
                添加字段“OD_code”：基于新的 adcode 和 placecode 值创建“OD_code”字段。
                计数并合并：对每个dataframe的OD_code进行计数，然后合并所有的计数结果。
                导出最终结果为csv：将合并后的结果导出为一个新的CSV文件。


                判断符号是本地还是外地符号,地级市前4位是否相同,直辖市是前3位是否相同
                如果不相同，提取为一条符号流，将外地符号用于构造符号流
                将相同的符号流进行合并，并累积所有符号流
                则
               
"""
import os
import glob
import pandas as pd


def process_csv(file_path):
    # 读取CSV文件
    df_raw = pd.read_csv(file_path,header=0)
    
    # 将省直辖县级市、港澳台纳入考虑范围之外
    # 找出df中需要删除的行
    rows_to_delete = df_raw['placecode'].isin(df_provincialcounties['adcode']) | df_raw['adcode'].isin(df_provincialcounties['adcode'])
    # 使用逻辑否定(~)来保留不需要删除的行
    df= df_raw[~rows_to_delete]
    
    #添加条件判断句，修改前打印是否满足条件
    
    #将莱芜区370116处理为莱芜市371200
    df.loc[:, 'placecode'] = df['placecode'].astype(str).str[:6].replace('370116','371200')
    #将那曲地区542400处理为那曲市540600
    # contains_value = df['adcode'].astype(str).str.contains('542400')
    # if contains_value.any(): 
    #     print("file_path")
    # df.loc[:, 'adcode'] = df['adcode'].astype(str).str[:6].replace('542400','540600')
    # 截取adcode和placecode的前4位，并根据条件修改值
    df.loc[:, 'adcode'] = df['adcode'].astype(str).str[:4].replace(['^31..', '^11..', '^12..', '^50..','5424'], ['3100', '1100', '1200', '5000','5406'], regex=True)#直辖市

    
    df.loc[:, 'placecode'] = df['placecode'].astype(str).str[:4].replace({
        #'1101': '1100', '1201': '1200', '3101': '3100', '5001': '5000',#直辖市
        '1300': '1301', '1400': '1401', '1500': '1501', #省会城市
        '2100': '2101', '2200': '2201', '2300': '2301', #省会城市
        '3200': '3201', '3300': '3301', '3400': '3401', #省会城市
        '3500': '3501', '3600': '3601', '3700': '3701', #省会城市
        '4100': '4101', '4200': '4201', '4300': '4301', #省会城市
        '4400': '4401', '4500': '4501', '4600': '4601', #省会城市
        '5100': '5101', '5200': '5201', '5300': '5301', #省会城市
        '5400': '5401',
        '6100': '6101', '6200': '6201', '6300': '6301', #省会城市
        '6400': '6401', '6500': '6501'#省会城市
        })
    
    df.loc[:, 'placecode'] = df['placecode'].astype(str).str[:4].replace(['^31..', '^11..', '^12..', '^50..'], ['3100', '1100', '1200', '5000'], regex=True)#直辖市
    

    # 删除adcode和placecode相同的行
    df = df[df['adcode'] != df['placecode']]

    # 添加OD_code字段
    df['OD_code'] = df['placecode'] + '00_' + df['adcode']+'00'

    return df

#读取省直辖县级行政单位的符号与编码
df_provincialcounties = pd.read_csv('data\input\provincialcounties.csv',header=0)
# 设定文件夹路径
folder_path = 'data\output\extractresult'  # 替换为你的文件夹路径

# 获取所有CSV文件路径
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

# 初始化一个空的DataFrame来存储最终计数结果
all_counts = []

# 处理每个CSV文件
for file in csv_files:
    df = process_csv(file)
    # 计算OD_code的计数
    counts = df['OD_code'].value_counts().reset_index()
    counts.columns = ['OD_code', 'count']
    # 合并计数
    all_counts.append(counts)

# 合并所有dataframe的OD_code计数
final_counts = pd.concat(all_counts, ignore_index=True).groupby('OD_code').sum().reset_index()
final_counts.to_csv('data\output\OD_code_counts.csv', index=False)

split_df = final_counts['OD_code'].str.split('_', expand=True)
final_counts['Ocity']= split_df[0].astype(int)
final_counts['Dcity']= split_df[1].astype(int)
#添加符号流的其他信息, Ocity, O_adcode, O_X, O_Y, Dcity, D_adcode, D_X, D_Y,
df_cities = pd.read_csv('data\output\city_geocode.csv',header=0)
df_cities.drop(['gcj_x','gcj_y'], axis=1, inplace=True)
merged_df = final_counts.merge(df_cities, left_on='Ocity', right_on='adcode',how='outer')
merged_df = merged_df.rename(columns={'fullname': 'Ocity_name','wgs_x':'O_X', 'wgs_y':'O_Y'})
df_symbolflow = merged_df.merge(df_cities, left_on='Dcity', right_on='adcode',how='outer')
df_symbolflow = df_symbolflow.rename(columns={'fullname': 'Dcity_name','wgs_x':'D_X', 'wgs_y':'D_Y'})

# 导出结果为CSV
df_symbolflow.to_csv('data\output\Symbolicflows.csv', index=False)

