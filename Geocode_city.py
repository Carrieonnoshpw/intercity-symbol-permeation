import os
import requests
import pandas as pd
from chinacoordtran import gcj02towgs84
def get_city_info(row, amap_key):
    city_name = row['fullname']  
    try:
        url = f'https://restapi.amap.com/v3/geocode/geo?address={city_name}&output=JSON&key={amap_key}'
        response = requests.get(url)
        data = response.json()
        if data and data['geocodes']:
            location = data['geocodes'][0]['location'].split(',')
            lng, lat = map(float, location)
            wgs84_lng, wgs84_lat = gcj02_to_wgs84(lng, lat)
            return {
                #'city_name': city_name,  # 添加城市名称
                #'adcode': data['geocodes'][0]['adcode'],
                'gcj_x': lng,
                'gcj_y': lat,
                'wgs_x': wgs84_lng,
                'wgs_y': wgs84_lat,
                    
            }
        else:
            return None
    except Exception as e:
        print(f"Error fetching data for {city_name}: {e}")
        return None
    
def gcj02_to_wgs84(lng, lat):
    gcj02towgs84Instance = gcj02towgs84()
    result =gcj02towgs84Instance.CoordTran(lng, lat)
    return(result.X, result.Y)
df= pd.read_csv('data\input\AMap_adcode.csv', header=0)
# 提取直辖市和地级市
# 直辖市条件：adcode以00结尾，且为110000、120000、310000、500000之一
# 地级市条件：adcode以00结尾，但不是110000、120000、310000、500000
direct_municipalities = df[df['adcode'].isin([110000, 120000, 310000, 500000])]
prefecture_level_cities = df[(df['adcode'] % 100 == 0) & ~df['adcode'].isin([110100, 120100, 310100, 500100]) & (df['adcode'] % 10000 != 0)]
# 合并直辖市和地级市
df_cities = pd.concat([direct_municipalities, prefecture_level_cities]).reset_index(drop=True)
# 删除重庆市郊县
df_cities = df_cities[~df_cities['fullname'].str.contains('重庆市郊县')]
#城市的地理编码
amap_key = '7a4a38d38a85569db6bb34c536a7f45b'#'您的高德API Key'
# df_cities.to_csv('data/test.csv')
df_cities[['gcj_x','gcj_y','wgs_x','wgs_y']] = df_cities.apply(get_city_info, args=(amap_key,),axis=1, result_type='expand')

#更新莱芜市信息,高德api返回的莱芜市地理编码与济南市相同
# 为每列指定更新值
update_values = {'gcj_x':117.675828,'gcj_y':36.214895,'wgs_x':117.66994899263214, 'wgs_y':36.21489948744972}  # 根据需要添加更多列
# 更新fullname为'莱芜市'的行
df_cities.loc[df_cities['fullname'] == '莱芜市', list(update_values.keys())] = list(update_values.values())

# 导出为CSV文件
output_file_path = 'data\output\city_geocode.csv'
# 检查目录是否存在，如果不存在则创建
output_dir = os.path.dirname(output_file_path)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
df_cities.to_csv(output_file_path, index=False)
print(f"城市信息已导出到 {output_file_path}")
