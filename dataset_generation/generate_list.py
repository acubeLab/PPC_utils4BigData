#!/usr/bin/env python3

import pandas as pd
import glob
import os

df = pd.DataFrame(
    columns=['blob_hash', 'blob_bytes', 'blob_path', 'local_path'])

#for f in glob.glob('/home/swh/50GiB_github/blobs_all/*.csv'):
for f in glob.glob('/home/swh/50GiB_github/NEW/blobs/*.csv'):
    #print(f)
    df_tmp = pd.read_csv(f,
                         #on_bad_lines='skip',
                         on_bad_lines='warn',
                         engine='python',
                         encoding_errors='ignore') # quote char?


    #reponame = os.path.basename(f).split('.')[0]
    assert(f.endswith('.csv'))
    reponame = os.path.basename(f)[:-(len('.csv'))]

    # print(reponame)
    df_tmp['local_path'] = reponame
    df = pd.concat([df, df_tmp])
    # print(df.head())

print(df.head())
print(df.drop_duplicates('blob_hash')['blob_bytes'].sum())

# create a new dataframe
df_new = pd.DataFrame(
    columns=['swh_id', 'file_id', 'length', 'filename', 'filepath', 'local_path'])

df_new['file_id'] = df['blob_hash']
df_new['length'] = df['blob_bytes']
df_new['filepath'] = df['blob_path']
df_new['filename'] = df['blob_path'].apply(os.path.basename)
df_new['local_path'] = df['local_path']
# swh_id can be easly computed from file_content but here is unecessary
df_new['swh_id'] = '0' # dummy value. Blobs from GitHub do not have swh_id by default

df_new.reset_index(drop=True, inplace=True)
print(df_new.head())

print(f"Before drop duplicates: {df_new['length'].sum()} in {round(df_new['length'].sum() / (2**30), 2)} GiB)") 

# # Create an empty DataFrame to store duplicates
# duplicates_df = pd.DataFrame(columns=df_new.columns)

# # Find duplicates based on 'file_id'
# duplicate_mask = df_new.duplicated(subset='file_id', keep=False)

# # Store duplicates in the new DataFrame
# duplicates_df = df_new[duplicate_mask].copy()

# # Function to find differences and store them in a new column
# def find_differences(row):
#     first_occurrence = df_new[df_new['file_id'] == row['file_id']].iloc[0]
#     differences = {col: (first_occurrence[col], row[col]) for col in df_new.columns if first_occurrence[col] != row[col]}
#     return differences

# # Apply the function to find differences for each row of duplicates
# duplicates_df['differences'] = duplicates_df.apply(find_differences, axis=1)


df_new.drop_duplicates('file_id', inplace=True)
print(f"Alfter drop duplicates: {df_new['length'].sum()} in {round(df_new['length'].sum() / (2**30), 2)} GiB)") 

df_new.to_csv('50GiB_github.csv', index=False)

#duplicates_df.to_csv('DUPLICATE_IN_50GiB_github.csv', index=False)
