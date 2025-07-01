# -*- coding: utf-8 -*-
"""
Created on Mon Jun 16 20:40:35 2025

@author: 80577
"""

import pandas as pd
df=pd.read_parquet(r'D:\pnas\0616_pnas.parquet')
df['is_inbred'] = (df['last_institution_id'] == df['degree_institution_id']).astype(int)
inbred_mean_by_gender = df.groupby('author_gender')['is_inbred'].mean()
grouped = df.groupby(['last_flow_year', 'author_gender'])['is_inbred'].agg(
    ['mean', 'count', 'sum']
).rename(columns={
    'mean': 'inbred_rate',
    'count': 'sample_size',
    'sum': 'inbred_count'
}).unstack()

# 整理多级列索引
grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
grouped.to_csv(r'D:\pnas\0616_year_inbred.csv')
result = df.groupby(['rank_type', 'author_gender'])['is_inbred'].mean().unstack()
result2 = df.groupby(['domain', 'author_gender'])['is_inbred'].mean().unstack()
result3 = df.groupby(['last_institution_country', 'author_gender'])['is_inbred'].mean().unstack()
result3['diff']=result3['Female']-result3['Male']
result2['diff']=result2['Female']-result2['Male']
result2.to_csv(r'D:\pnas\study1\domain.csv')
result3.to_csv(r'D:\pnas\study1\country.csv')
result.to_csv(r'D:\pnas\study1\rank.csv')

df=pd.read_csv(r'D:\pnas\cv.csv', encoding='latin1')
df['gender'] = df['gender'].replace({1: 'male', 2: 'female'})
df = df[df['gender'].isin(['male', 'female'])]
import numpy as np
df['is_inbred'] = np.where(df['work_id'] == df['dc1_id'], 1, 0)
# 按 gender 分组，计算 is_inbred 的均值
inbred_mean_by_gender = df.groupby('gender')['is_inbred'].mean()

# 输出结果
result = df.groupby(['gender', 'dc1_year2'])['is_inbred'].agg(
    inbred_rate='mean',  # 计算 is_inbred 的均值（近亲比例）
    sample_size='count'  # 计算每组的样本数量
).reset_index()
result.to_csv(r'D:\pnas\study1\0617_cv_year_inbred.csv')
result1 = df.groupby(['subject_code', 'gender'])['is_inbred'].agg(
    inbred_rate='mean',    # 计算 is_inbred 的均值（近亲比例）
    sample_size='count'    # 计算每组的样本数量
).reset_index()
result1.to_csv(r'D:\pnas\study1\0617_cv_subject_inbred.csv')
result2 = df.groupby(['country_code', 'gender'])['is_inbred'].agg(
    inbred_rate='mean',    # 计算 is_inbred 的均值（近亲比例）
    sample_size='count'    # 计算每组的样本数量
).reset_index()
result2.to_csv(r'D:\pnas\study1\0617_cv_country_inbred.csv')
result3 = df.groupby(['work_rank2', 'gender'])['is_inbred'].agg(
    inbred_rate='mean',    # 计算 is_inbred 的均值（近亲比例）
    sample_size='count'    # 计算每组的样本数量
).reset_index()
result3.to_csv(r'D:\pnas\study1\0617_cv_rank_inbred.csv')
df=pd.read_parquet(r'D:\pnas\0619_pnas.parquet')
a=df1['x'].value_counts()
df['x']=df['publication_year']-df['graduate_year']
df['last_gra']=df['last_flow_year']-df['graduate_year']
df1=df[df['x']<=0]
df1=df1[df1['x']>=-5]

df1 = df1.rename(columns={'x': 'year'})
df1['author_gender'] = df1['author_gender'].replace({'Female': 1, 'Male': 0})
fillna_cols = ['article_count', 'journal_normalized_citation_impact', 'cited_per_year']

# 填充缺失值为0
df1[fillna_cols] = df1[fillna_cols].fillna(0)
winsorize_cols = ['article_count', 'journal_normalized_citation_impact', 'cited_per_year']

# 定义缩尾函数
def winsorize_series(series, lower=0.05, upper=0.95):
    lower_bound = series.quantile(lower)
    upper_bound = series.quantile(upper)
    return series.clip(lower=lower_bound, upper=upper_bound)

# 对每个变量进行缩尾处理
for col in winsorize_cols:
    df1[col] = winsorize_series(df1[col])
df1['rank_mean_z'] = (df1['rank_mean'] - df1['rank_mean'].mean()) / df1['rank_mean'].std()
df1['country_code'] = pd.factorize(df1['last_institution_country'])[0] + 1
df1.to_csv(r'D:\pnas\study2\0621_model.csv')

###################################################################################
df['x']=df['publication_year']-df['last_flow_year']
df1=df[df['x']>0]
df1=df1[df1['x']<=10]
df1 = df1.rename(columns={'x': 'year'})
df1['author_gender'] = df1['author_gender'].replace({'Female': 1, 'Male': 0})
fillna_cols = ['article_count', 'journal_normalized_citation_impact', 'cited_per_year']

# 填充缺失值为0
df1[fillna_cols] = df1[fillna_cols].fillna(0)
winsorize_cols = ['article_count', 'journal_normalized_citation_impact', 'cited_per_year']
def winsorize_series(series, lower=0.05, upper=0.95):
    lower_bound = series.quantile(lower)
    upper_bound = series.quantile(upper)
    return series.clip(lower=lower_bound, upper=upper_bound)

# 对每个变量进行缩尾处理
for col in winsorize_cols:
    df1[col] = winsorize_series(df1[col])
df1['rank_mean_z'] = (df1['rank_mean'] - df1['rank_mean'].mean()) / df1['rank_mean'].std()
df1['country_code'] = pd.factorize(df1['last_institution_country'])[0] + 1
df1.to_csv(r'D:\pnas\study3\0622_model.csv')
#################################################################################
df=pd.read_parquet(r'D:\pnas\0624_pnas_name.parquet')
df_ss=pd.read_excel(r'D:\pnas\Soical Sciences Laureates.xlsx')


df=df[df['dc1_year2']!=0]
df=df[df['gender']!='.']
grouped_mean = df.groupby(['work_rank2', 'gender', 'is_inbred'])[['BS1_rank1', 'dc1_rank1']].mean()
grouped_mean.to_csv(r'D:\pnas\study2\cv_rank1.csv')

df=pd.read_csv(r'D:\pnas\study2\0621_model.csv')
df1 = df.groupby('author_id', as_index=False).agg({
    'article_count': 'sum',
    'journal_normalized_citation_impact': 'sum',
    'cited_per_year': 'sum'
})
df2 = df.drop_duplicates(subset='author_id', keep='first')

df1=pd.merge(df1,df2,how='left')
df2 = df2.drop(columns=['article_count', 'journal_normalized_citation_impact', 'cited_per_year'])

# 2. 将 df1 合并到 df2_dropped（按 author_id 匹配）
df2 = pd.merge(df2, df1, on='author_id', how='left')
gender_mean = df2.groupby(['rank_type', 'author_gender'])[['article_count', 'journal_normalized_citation_impact', 'cited_per_year']].mean()
gender_mean.to_csv(r'D:\pnas\study2\oa_pub1.csv')
gender_mean = df2.groupby(['rank_type', 'author_gender','is_inbred'])[['article_count', 'journal_normalized_citation_impact', 'cited_per_year']].mean()
gender_mean.to_csv(r'D:\pnas\study2\oa_pub2.csv')
count = len(df_ss_fuzz[df_ss_fuzz['match_score'] > 90])
print(f"匹配分数 > 90 的记录数: {count}")
df=pd.read_parquet(r'D:\pnas\0616_pnas.parquet')
df = df.drop_duplicates(subset=['author_id'])

# 根据graduate_year创建time_period列
def create_time_period(year):
    if pd.isnull(year):
        return None
    year = int(year)
    if 1988 <= year <= 1996:
        return 1
    elif 1997 <= year <= 2005:
        return 2
    elif 2006 <= year <= 2014:
        return 3
    elif 2015 <= year <= 2023:
        return 4
    else:
        return None

df['time_period'] = df['graduate_year'].apply(create_time_period)
df['time_period'].value_counts()
df['rank_mean_z'] = (df['rank_mean'] - df['rank_mean'].mean()) / df['rank_mean'].std()
df['country_code'] = pd.factorize(df['last_institution_country'])[0] + 1
df.to_csv(r'D:\pnas\0624_oa.csv')
df=df[df['last_institution_country']]