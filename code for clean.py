# -*- coding: utf-8 -*-
"""
Created on Fri Jun 13 09:23:34 2025

@author: 80577
"""

import pandas as pd
df=pd.read_parquet(r'D:\human\0611_orcid_degree_human.parquet')
df=pd.merge(df,df_last_year)
merged_df['degree_institution_type'].value_counts()

df_rank = pd.read_csv(r'D:\pnas\OA_works_authorships_institution.csv', encoding='latin1')
merged_df = df.join(
    df_rank.set_index('institution_id'), 
    on='last_institution_id',
    how='left'
)
last_four_cols = merged_df.iloc[:, -4:]

# 检查这四列是否全部为 NaN
all_nan_mask = last_four_cols.isna().all(axis=1)

# 统计符合条件的行数
count_all_nan = all_nan_mask.sum()

print(f"最后四列全部为 NaN 的行数: {count_all_nan}")
import re
import random
#############
def fix_specific_range(range_val):
    """
    仅将 "数字¨C数字" 替换为 "数字-数字"，其他情况保持原样
    """
    if pd.isna(range_val):
        return range_val  # 保留 NaN
    
    if isinstance(range_val, (int, float)):
        return range_val  # 保留纯数字
    
    # 仅匹配 "数字¨C数字" 的格式
    if re.fullmatch(r'\d+¨C\d+', str(range_val).strip()):
        return str(range_val).replace('¨C', '-')
    else:
        return range_val  # 其他情况不修改

# 直接修改倒数第二列（原地替换）
merged_df.iloc[:, -2] = merged_df.iloc[:, -2].apply(fix_specific_range)

def generate_random(range_val):
    if pd.isna(range_val) or not re.fullmatch(r'\d+-\d+', str(range_val)):
        return range_val
    low, high = map(int, range_val.split('-'))
    return random.randint(low, high)

merged_df.iloc[:, -2] = merged_df.iloc[:, -2].apply(generate_random)
merged_df.iloc[:, -1] = merged_df.iloc[:, -1].apply(generate_random)
last_four_cols = merged_df.iloc[:, -4:]
print("转换前的数据类型：")
print(last_four_cols.dtypes)
target_countries = [
    'US', 'BR', 'IN', 'ES', 'GB', 'TR', 'AU', 'IT', 'DE', 'PL',
    'CA', 'MX', 'PT', 'KR', 'IR', 'FR', 'ID', 'SE', 'CO', 'CH'
]

# 标准化国家代码为大写
merged_df['last_institution_country'] = merged_df['last_institution_country'].str.upper()

# 过滤数据
merged_df = merged_df[merged_df['last_institution_country'].isin(target_countries)].copy()

# 验证
print(f"最终数据量: {len(merged_df)}")
print("国家分布:")
print(merged_df['last_institution_country'].value_counts().sort_index())
last_four_cols = merged_df.columns[-4:]
merged_df[last_four_cols] = merged_df[last_four_cols].apply(pd.to_numeric, errors='coerce')

# 2. 计算均值（自动跳过NaN），生成新列
merged_df['rank_mean'] = merged_df[last_four_cols].mean(axis=1)

# 3. 处理全为NaN的行（可选：将其设为特定值如NaN或空字符串）
merged_df['rank_mean'] = merged_df['rank_mean'].replace({np.nan: ''})

# 4. 验证结果
print("最后四列和均值列示例：")
print(merged_df[list(last_four_cols) + ['rank_mean']].head(10))
merged_df = merged_df.dropna(subset=['rank_mean'])

# 如果空值存储为空字符串''
merged_df = merged_df[merged_df['rank_mean'] != '']
a=merged_df['rank_mean'].value_counts()
df1=merged_df[merged_df['rank_mean']<=2000]
df['is_inbred'] = (df['last_institution_id'] == df['degree_institution_id']).astype(int)
bins = [0, 200, 500, 1000, 2000]  # 左开右闭区间
labels = [1, 2, 3, 4]
df['rank_type'] = pd.cut(
    df['rank_mean'],
    bins=bins,
    right=True,    # 包含右边界（如200属于1组）
    include_lowest=True,  # 包含最小值
    labels=labels
)
df1.to_parquet(r'D:\pnas\0616_pnas.parquet')
##############################################################
df=pd.read_excel(r'D:\human\简历\17.7全球大学教师简历数据.xlsx')
df0=df.iloc[:100]
df = df[~(df['DC1_ID'].isna() | (df['DC1_ID'] == '')) | ~(df['DC2_ID'].isna() | (df['DC2_ID'] == ''))]
df['DC1_ID'] = df['DC1_ID'].fillna(df['DC2_ID'])
df['DC1_Loc1'] = df['DC1_Loc1'].fillna(df['DC2_Loc1'])
df['DC1_Loc2'] = df['DC1_Loc2'].fillna(df['DC2_Loc2'])
df['DC1_Loc2'] = df['DC1_Loc2'].fillna(df['DC2_Loc2'])
print(df.columns)
职位', 'WORK_ID',国家代码', '学科代码', 'WORK_ID.1','性别', 'BS1', 'MS1', 'DC1','DC1_year1', 'DC1_year2','DC1_ID',DC1_rank1', 'DC1_rank2', 'DC1_Loc2',
columns_to_keep = [
    '职位', 'WORK_ID', '国家代码', '学科代码', 'WORK_ID.1', '性别', 
    'BS1', 'MS1', 'DC1', 'DC1_year1', 'DC1_year2', 'DC1_ID', 
    'DC1_rank1', 'DC1_rank2', 'DC1_Loc2'
]

df = df[columns_to_keep]

# 重命名列
column_mapping = {
    '职位': 'position',
    'WORK_ID': 'work_id',
    '国家代码': 'country_code',
    '学科代码': 'subject_code',
    'WORK_ID.1': 'work_id_1',
    '性别': 'gender',
    'BS1': 'bs1',
    'MS1': 'ms1',
    'DC1': 'dc1',
    'DC1_year1': 'dc1_year1',
    'DC1_year2': 'dc1_year2',
    'DC1_ID': 'dc1_id',
    'DC1_rank1': 'dc1_rank1',
    'DC1_rank2': 'dc1_rank2',
    'DC1_Loc2': 'dc1_loc2'
}
df = df.rename(columns=column_mapping)
df.to_csv(r'D:\pnas\cv.csv')
df=pd.read_csv(r'D:\pnas\cv.csv', encoding='latin1')
df['WORK_rank2'].value_counts()
df = df[['BS1_rank1', 'WORK_rank2']]
df_combined = pd.concat([df1, df], axis=1)
df.to_csv(r'D:\pnas\a.csv')
