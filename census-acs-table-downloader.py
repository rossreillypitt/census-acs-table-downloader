import requests
import zipfile
from io import BytesIO
import pandas as pd
import sys
import numpy as np
import os.path
import pip
import openpyxl

year = 2021
st = "pa"
table_id = "B28002"

sys.argv = "file", 2021, "pa", "B28002"

if len(sys.argv) > 1:
    try:
        year = sys.argv[1]
        st = sys.argv[2].lower()
        table_id = sys.argv[3]
    except IndexError: 
        print("Please enter three arguments when running the program: year, state abbreviation," 
              "and desired table number.")
        exit()
else:
    print("Please enter three arguments when running the program: year, state abbreviation," 
            "and desired table number.")
    exit()   

base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/'
tract_block_loc = '/Tracts_Block_Groups_Only/'
all_other_loc = '/All_Geographies_Not_Tracts_Block_Groups/'
post_2020_add_on = '/sequence-based-SF' if year > 2020 else ''
# tract_block_url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/data/5_year_seq_by_state/Pennsylvania/Tracts_Block_Groups_Only/'
tract_block_url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}{post_2020_add_on}/data/5_year_seq_by_state/Pennsylvania/Tracts_Block_Groups_Only/'
all_other_url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{year}/data/5_year_seq_by_state/Pennsylvania/All_Geographies_Not_Tracts_Block_Groups/'

#creates a dictionary with state postal abbreviations as keys and full state names (styled as they would be in Census URLs) as the values.
#Note: double check that all of these are as they appear in the census URLs.
state_names = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'NewHampshire', 'NewJersey', 'NewMexico', 'NewYork', 'NorthCarolina', 'NorthDakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'PuertoRico', 'RhodeIsland', 'SouthCarolina', 'SouthDakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'WestVirginia', 'Wisconsin',  'Wyoming']
state_abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
state_name_abbr_dict = {}
for x in range(len(state_names)):
    state_name_abbr_dict[state_abbrevs[x].lower()] = state_names[x]

#creates dictionary with Logrecnos as keys and GEOIds as the values.
geog_file_name = f'{tract_block_url}g{year}5pa.csv'
geog_file_csv_output = pd.read_csv(geog_file_name, header=None, dtype = str)
logrecno_dict = {}
geog_dict = geog_file_csv_output.to_dict('records')

################## NEW code below ###################################

logrecno_dict = {}
for i in range(len(geog_dict)):
    logrecno_dict[geog_dict[i][4]] = (f'{geog_dict[i][48][:5]}00{geog_dict[i][48][5:]}', geog_dict[i][49])
    
################# Back to old code ##################################

#creates table of labels for sequences
zip_file_name = f'{year}_5yr_Summary_FileTemplates.zip'
zipped_file = requests.get(f'{base_url}{year}{post_2020_add_on}/data/{zip_file_name}')
read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
seq_header_list = []
# url_capture_idx = 0
# for item in read_file.namelist():
#     try:
#         if "seq" in item.lower():
#             full_filename = item
#             break
#     except Exception as e:
#         print(e.args, "check line 77, there's a problem with the zip file structure")
#         quit()
# format = full_filename.split(".")[-1]
# path_in_zip = "/".join(full_filename.split("/")[:-1])
# if len(path_in_zip) > 0:
#     path_in_zip += "/"
# file_seq_style = full_filename.split("/")[-1][:3]

list_of_files_to_read = [filename for filename in read_file.namelist() if "seq" in filename.lower()]
for filename in list_of_files_to_read:
    num = int(filename.lower().split("seq")[-1].split(".")[0])
    try:
        seq_header_txt = pd.read_excel(read_file.read(filename), header=None)
        seq_header_txt = seq_header_txt.iloc[:, 6:].transpose()
        seq_header_txt.loc[:, len(seq_header_txt.columns)]=f'{num:04d}'
        seq_header_list.append(seq_header_txt)
    except ValueError:
        print("had one of those valueerrors")
        pass

# for x in range(1, len(read_file.namelist())):
#     try:
#         file_to_read = f'{path_in_zip}{file_seq_style}{x}.{format}'
#         seq_header_txt = pd.read_excel(read_file.read(file_to_read), header=None)
#         seq_header_txt = seq_header_txt.iloc[:, 6:].transpose()
#         seq_header_txt.loc[:, len(seq_header_txt.columns)]=f'{x:04d}'
#         seq_header_list.append(seq_header_txt)
#     except ValueError:
#         pass
seq_header_dF = pd.concat(seq_header_list)
seq_header_dF.reset_index(drop=True)

seq_header_file_name = f'{year}_acs_sequence_labels.csv'
if os.path.exists(seq_header_file_name):
    pass
else:
    seq_header_dF.to_csv(seq_header_file_name)

############# NEW code below ##########################

selected_table_info = []
for x in range(len(seq_header_dF)):
    if table_id in seq_header_dF.iloc[x][0]:
        selected_table_info.append(seq_header_dF.iloc[x])
        
### put the selected_table_info[0][2] into a set, and then have different rules if the set is len == 1, len == 2, etc. 
sequences = set()
for item in selected_table_info:
    sequences.add(item[2])

seq_end = f'{int(max(sequences)) + 1:04d}'
sequences = list(sequences)
sequences.sort()

y=0
column_list = []
for item in sequences:
    y=0
    column_staging_list = []
    for x in range(len(seq_header_dF)):
        if item == seq_header_dF.iloc[x,2]:  
            if table_id in seq_header_dF.iloc[x][0]:
                column_staging_list.append(y)
            y+=1
    column_list.append(column_staging_list)

############# copied code below ##########################

def acs_5yr_csv_output_all(year, st_abbv, sequences, seq_end):
    def grabber(geog_type, sequences):
        flag = True
        concat_dF = pd.DataFrame([])
        ongoing_estimate_dF = pd.DataFrame([])
        ongoing_margin_dF = pd.DataFrame([])
        for x in range(len(sequences)):
            staging_estimate_dF = pd.DataFrame([])
            staging_margin_dF = pd.DataFrame([])
            zip_file_name = f'{year}5{st_abbv}{sequences[x]}000.zip'
            e_file_name = f'e{year}5{st_abbv}{sequences[x]}000.txt'
            m_file_name = f'm{year}5{st_abbv}{sequences[x]}000.txt'
            tract_file_check = requests.head(
                f'{base_url}{year}{post_2020_add_on}/data/5_year_seq_by_state/{state_name_abbr_dict[st_abbv]}{geog_type}{zip_file_name}')
            if tract_file_check.status_code == 200:
                flag = True
                zipped_file = requests.get(
                    f'{base_url}{year}{post_2020_add_on}/data/5_year_seq_by_state/{state_name_abbr_dict[st_abbv]}{geog_type}{zip_file_name}')
                read_file = zipfile.ZipFile(BytesIO(zipped_file.content))

                estimate_seq_txt = pd.read_csv(read_file.open(e_file_name), header=None,
                                                dtype=str)
                margin_seq_txt = pd.read_csv(read_file.open(m_file_name), header=None,
                                                dtype=str)
                ongoing_estimate_dF = pd.concat([ongoing_estimate_dF, estimate_seq_txt], axis=1).reset_index(drop=True)
                ongoing_margin_dF = pd.concat([ongoing_margin_dF, margin_seq_txt], axis=1).reset_index(drop=True)
            else:
                print(f'Could not access sequence #{sequences[x]}')
        return ongoing_estimate_dF, ongoing_margin_dF

    tract_block_estimate_dF, tract_block_margin_dF = grabber(tract_block_loc, sequences)
    all_other_estimate_dF, all_other_margin_dF = grabber(all_other_loc, sequences)
    combined_estimate_dF = pd.concat([tract_block_estimate_dF, all_other_estimate_dF]).reset_index(drop=True)
    combined_margin_dF = pd.concat([tract_block_margin_dF, all_other_margin_dF]).reset_index(drop=True)
    return combined_estimate_dF, combined_margin_dF

table_111 = acs_5yr_csv_output_all(year, st, sequences, seq_end)

############# NEW code below ##########################

final_columns = []
for y in range(len(sequences)):
    for x in range(len(table_111[0].iloc[1,])):
        if table_111[0].iloc[1,x] == sequences[y]:
            final_columns.extend(item + (x+2) for item in column_list[y])
            break

end_list_e = []
end_list_m = []
for x in range(len(final_columns)):
    end_list_e.append(table_111[0].iloc[:,final_columns[x]])
    end_list_m.append(table_111[1].iloc[:, final_columns[x]])
end_df_e = pd.DataFrame(end_list_e).reset_index(drop=True)
end_df_m = pd.DataFrame(end_list_m).reset_index(drop=True)

final_geog_staging_list = []
for x in range(len(table_111[0].iloc[:,5])):
    final_geog_staging_list.append([table_111[0].iloc[x,5], logrecno_dict[table_111[0].iloc[x,5]][0], logrecno_dict[table_111[0].iloc[x,5]][1]])
final_geog_staging_dF = pd.DataFrame(final_geog_staging_list) 

needed_estimate_table = pd.concat([table_111[0].iloc[:, 0:4], final_geog_staging_dF, end_df_e.T], axis=1)
needed_margin_table = pd.concat([table_111[1].iloc[:, 0:4], final_geog_staging_dF, end_df_m.T], axis=1)
final_table_columns = ["Survey", "FileType", "State", "Chariter", "Logrecno", "GEOID", "Location"]
for x in range(len(selected_table_info)):
    final_table_columns.append(f'{selected_table_info[x][0]}: {selected_table_info[x][1]}')
needed_estimate_table.columns, needed_margin_table.columns = final_table_columns, final_table_columns

est_table_file_name = f'{year}_acs_table_{table_id}_estimate.csv'
moe_table_file_name = f'{year}_acs_table_{table_id}_margin_of_error.csv'

needed_estimate_table.to_csv(est_table_file_name)
needed_margin_table.to_csv(moe_table_file_name)
