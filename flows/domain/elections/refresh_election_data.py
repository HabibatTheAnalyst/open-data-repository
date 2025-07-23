# Import necessary libraries
from prefect import flow, task, serve
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import NoCredentialsError
from io import StringIO, BytesIO
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from prefect.blocks.system import Secret
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# Connect to Amazon S3
# Provide your AWS credentials with Prefect blocks
aws_access_key_id = Secret.load('aws-access-key-id').get()
aws_secret_access_key = Secret.load('aws-secret-access-key').get()
region_name = 'eu-west-1'
bucket_name = 'stears-flourish-data'

# Create an S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Create Google Drive API service
SCOPES = ['https://www.googleapis.com/auth/drive']  # Scope required to access Google Drive (read and write access)
google_json = Secret.load(
    'automating-election-google-drive-api-secret-block').get()  # Block was created in prefect.stears.co
service_account_info = json.loads(google_json)
service_account_creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=service_account_creds)

# Access results folder
Results_folder_file_id = '1Wmr8gXnBfAgHRTgsPOdK-45htWhlBqhj'
try:
    results = drive_service.files().list(
        q=f"'{Results_folder_file_id}' in parents",
        fields="files(name, id)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        # Turn the data into a dataframe
        country_name_fileid_data = [{'File_Name': item['name'], 'File_ID': item['id']} for item in items]
        country_name_fileid_data_df1 = pd.DataFrame(country_name_fileid_data)

        # Split file name on delimiter to remove "All-data-" prefix
        split_name = country_name_fileid_data_df1['File_Name'].str.split('-', expand=True)
        split_name = split_name.drop([0, 1], axis=1)
        split_name = split_name[2].str.lower()

        country_name_fileid_data_df = pd.concat([split_name, country_name_fileid_data_df1['File_ID']], axis=1)
        country_name_fileid_data_df = country_name_fileid_data_df.rename(columns={2: "File_Name"})

        # Convert df to dictionary
        country_name_fileid_data_dict = country_name_fileid_data_df.set_index('File_Name')['File_ID'].to_dict()
except Exception as e:
    print(f"An error occurred: {e}")

african_level_sheet_path = '1KsITG1CTbes0E0rj34q3zrc-NbkUm15b'
term_limits_sheet_path = '1kndjVWmJ98ucRHkv0xdofQVpaWBTlbbp'

elections_df = None
countries_df = None
population_df = None
democracy_level_df = None
gdp_df = None
coup_df = None
term_limits_df = None
list_of_all_s3_urls = []

# Get file from Google Drive
@task
def download_file_from_drive(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        print(f"Failed to download file with ID {file_id}: {e}")
        return None


@task
def setup():
    # Download file content from the given paths
    file_content_african_level = download_file_from_drive(african_level_sheet_path)
    file_content_term_limits = download_file_from_drive(term_limits_sheet_path)
        
    if file_content_african_level and file_content_term_limits:
        African_level_sheet = pd.ExcelFile(file_content_african_level)
        Term_limits_sheet = pd.ExcelFile(file_content_term_limits)

        sheets_dict = {}  # dictionary to hold sheets from both spreadsheets

        for sheet_name in African_level_sheet.sheet_names:  # read each sheet from both spreadsheets into the dictionary
            sheets_dict[sheet_name] = pd.read_excel(African_level_sheet, sheet_name=sheet_name)

        for sheet_name in Term_limits_sheet.sheet_names:
            sheets_dict[sheet_name] = pd.read_excel(Term_limits_sheet, sheet_name=sheet_name)

        global elections_df
        global countries_df
        global population_df
        global democracy_level_df
        global gdp_df
        global coup_df
        global term_limits_df
        global upcoming_elections
        global past_elections

        elections_df = sheets_dict['elections']
        countries_df = sheets_dict['countries']
        population_df = sheets_dict['population']
        democracy_level_df = sheets_dict['democracy_level']
        gdp_df = sheets_dict['gdp']
        coup_df = countries_df[['Country', 'State of Civilian Rule']]
        term_limits_df = sheets_dict['Term_limits']

        return True
    return False


@task
def generate_both_trackers():
    global elections_df
    
    def clean_date(date_value):
        if isinstance(date_value, str):
            date_value = date_value.replace('*', '')
            date_value = date_value.replace('Jan', 'January')
            date_value = date_value.replace('Feb', 'February')
            date_value = date_value.replace('Mar', 'March')
            date_value = date_value.replace('Apr', 'April')
            date_value = date_value.replace('Jun', 'June')
            date_value = date_value.replace('Jul', 'July')
            date_value = date_value.replace('Aug', 'August')
            date_value = date_value.replace('Sep', 'September')
            date_value = date_value.replace('Oct', 'October')
            date_value = date_value.replace('Nov', 'November')
            date_value = date_value.replace('Dec', 'December')
        return date_value

    # Apply the clean_date function to the 'Date' column
    elections_df['Date_cleaned'] = elections_df['Date'].apply(clean_date)
    elections_df['Date_new'] = pd.to_datetime(elections_df['Date_cleaned'], errors='coerce')
    elections_df['Extracted_Year'] = elections_df['Date_new'].dt.year

    # Initialize 'Status' column
    elections_df['Status'] = ''

    # Define the current year and month for comparison
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day

    # Determine the status
    def determine_status(row):
        if pd.isna(row['Date']) or row['Date_new'].year > current_year + 1:
            return 'Neither'
        elif row['Date_new'].year < current_year or (row['Date_new'].year == current_year and row['Date_new'].month < current_month) or (row['Date_new'].year == current_year and row['Date_new'].month == current_month and row['Date_new'].day + 3 <= current_day):
            return 'Past'
        elif row['Date_new'].year == current_year +1:
            return 'Upcoming'
        else:
            return 'Upcoming'

    elections_df['Status'] = elections_df.apply(determine_status, axis=1)

    def sort_dates(group):
        status = group.name
        if status == 'Past':
            return group.sort_values(by='Date_new', ascending=False)
        else:
            return group.sort_values(by='Date_new', ascending=True, na_position='last')

    # Group by 'Status'
    elections_df = elections_df.groupby('Status', group_keys=False).apply(sort_dates).reset_index(drop=True)

    # Drop unnecessary columns
    elections_df.drop(columns=['Date_cleaned','Date_new', 'Extracted_Year'], inplace=True)


    # Process the save path - function to upload the manipulated election dataframe to an S3 bucket
    def upload_election_tables_to_s3(processed_election_df, election_file_name):

        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        processed_election_df.to_csv(csv_buffer, index=False)

        try:
            # Upload the file
            s3_client.put_object(Bucket=bucket_name, Key=election_file_name, Body=csv_buffer.getvalue(), ContentType='text/csv')
            # print(f"{election_file_name} has been uploaded to {bucket_name}")
        except NoCredentialsError:
            print("Credentials not available")
            return False
        return True


    merge_tables = pd.merge(elections_df, democracy_level_df, on='Country', how='left')
    elections_table = pd.merge(merge_tables, countries_df[['Country', 'Stears URL']], on='Country', how='left')
    upcoming_elections = elections_table[elections_table['Status'] == 'Upcoming'] # get past elections
    past_elections = elections_table[elections_table['Status'] == 'Past']  # get past elections

    # process a row from each df and generate a formatted string based on the contents of 'Description' and 'Stears URL'
    def upcoming(df): # This only works on upcoming_elections
        def process_data(row):
            # it returns '_' if description is null and '' if stearsURL is null
            description = str(row['Description']) if pd.notna(row['Description']) else '-'
            link = str(row['Stears URL']) if pd.notna(row['Stears URL']) else ''
            if link:
                if link not in description:  # checks if the link is already in the description to avoid duplication
                    return f"{description} <br><br><a href='{link}'><b>View profile ➜</b></a>"
                else:
                    return description  # return the description if link is already appended
            else:
                return description  # return the description if url is not available

        df['Description'] = df.apply(process_data, axis=1)  # apply the function to the 'Description' column

        def __sorts_and_formats_date_as_string(row):
            if row['Date (placeholder)'] == 'Yes' and row['Date']:
                date_value = row['Date']
                if isinstance(date_value, str):
                    date_value = datetime.strptime(date_value, '%d %b %Y')
                return f"{date_value.strftime('%b %Y')}*"
            return row['Date']

        df['Date'] = df.apply(__sorts_and_formats_date_as_string, axis=1)

        df = df[['Country', 'Type', 'Date', 'Democracy', 'Description']]  # returns the required columns
        df = df.rename(columns={"Description": "What's at Stake", "Type": "Elections"})

        index = df.columns.get_loc('Democracy')  # get 'Democracy' index and add html to the column
        
        df.columns = [
            col + ' &#9432; >>EIU Democracy Index, 2024<br><br>The index is based on five categories: electoral process and pluralism, functioning of government, political participation, political culture, and civil liberties. Based on its 0-10 scores on a range of indicators within these categories, each country is classified as one of four types of regime: “full democracy”, “flawed democracy”, “hybrid regime” or “authoritarian regime."'
            if df.columns.get_loc(col) == index else col for col in df.columns]

        global upcoming_tracker_name 
        upcoming_tracker_name = 'africa-upcoming-tracker.csv'

        return df
    processed_upcoming_elections = upcoming(upcoming_elections)

    def past(df): # This only works on past_elections
        # process a row from df and generate a formatted string based on the contents of 'Description' and 'Stears URL'
        def process_past_data(row):
            # it returns '' if description is null and '' if stearsURL is null
            description = str(row['Description']) if pd.notna(row['Description']) else ''
            link = str(row['Stears URL']) if pd.notna(row['Stears URL']) else ''

            if link:
                if link not in description:  # checks if the link is already in the description to avoid duplication
                    return f"{description} <br><br><a href='{link}'><b>View results ➜</b></a>"
                else:
                    return description  # return the description if link is already appended
            else:
                return description  # return the description if url is not available

        df['Description'] = df.apply(process_past_data, axis=1)  # apply the function to the 'Description' column
        df = df[['Country', 'Type', 'Date', 'Democracy', 'Description']]  # return the required columns
        df = df.rename(columns={"Description": "Recap of significance and outcome", "Type": "Elections"})

        index = df.columns.get_loc('Democracy')  # get the index for 'Democracy'
        
        df.columns = [
            col + ' &#9432; >>EIU Democracy Index, 2024<br><br>The index is based on five categories: electoral process and pluralism, functioning of government, political participation, political culture, and civil liberties. Based on its 0-10 scores on a range of indicators within these categories, each country is classified as one of four types of regime: “full democracy”, “flawed democracy”, “hybrid regime” or “authoritarian regime."'
            if df.columns.get_loc(col) == index else col for col in df.columns]  # append the string to 'Democracy' column
        
        global past_tracker_name 
        past_tracker_name = 'africa-past-tracker.csv'

        return df
    processed_past_elections = past(past_elections)

    upload_election_tables_to_s3(processed_upcoming_elections, upcoming_tracker_name)
    upload_election_tables_to_s3(processed_past_elections, past_tracker_name)

    print(f'https://{bucket_name}.s3.amazonaws.com/{upcoming_tracker_name}')
    list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{upcoming_tracker_name}')
    print(f'https://{bucket_name}.s3.amazonaws.com/{past_tracker_name}')
    list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{past_tracker_name}')


@task
def generate_upcoming_points():
    global elections_df

    # merge tables
    merge_points = pd.merge(countries_df, elections_df, on='Country', how='left', suffixes=('', '_country'))
    merge_points.rename(columns={'Type': 'Elections', 'Stears URL': 'Profile'}, inplace=True)
    merge_points['Type'] = np.where(merge_points['Priority'] == 'Yes', 'Key race to watch', 'Other election')

    # get upcoming election points and required columns
    upcoming_points = merge_points[merge_points['Status'] == 'Upcoming'][
        ['Longitude', 'Latitude', 'Country', 'Type', 'Profile', 'Elections']]

    def upload_upcoming_points_to_s3():  # load to s3
        try:
            upcoming_points_name = 'africa-upcoming-points.csv'
            csv_buffer = StringIO()
            upcoming_points.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=bucket_name, Key=upcoming_points_name, Body=csv_buffer.getvalue(),
                                 ContentType='text/csv')
            file_url = f"https://{bucket_name}.s3.amazonaws.com/{upcoming_points_name}"
            print(f"File uploaded to {bucket_name}/{upcoming_points_name}")
            list_of_all_s3_urls.append(file_url)
            return file_url
        except NoCredentialsError:
            print("Credentials not available")
            return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    file_url = upload_upcoming_points_to_s3()
    print(f'File URL: {file_url}')
    list_of_all_s3_urls.append(file_url)
    upload_upcoming_points_to_s3()
    print('I am done!', 'generate_upcoming_points')


@task
def generate_africa_maps():
    def classify_democracy_age(date):
        if date in ["Non-democracy"]:
            return date
        else:
                date = pd.to_datetime(date, format='%B %Y')
                current_year = datetime.now().year
                democracy_age = current_year - date.year
                if democracy_age < 10:
                    return '<10 yrs'
                elif 10 <= democracy_age < 20:
                    return '10-19 yrs'
                elif 20 <= democracy_age < 40:
                    return '20-39 yrs'
                elif 40 <= democracy_age < 60:
                    return '40-59 yrs'
                elif 60 <= democracy_age < 80:
                    return '60-79 yrs'

    global countries_df
    global coup_df
    # apply function to column
    countries_df['African Map Democracy Age'] = countries_df['Date that current continuous democracy started (i.e. elections were held)'].apply(classify_democracy_age)

    # create columns for the african wide democracy age table
    def africa_wide_democracy_age(date):
        if pd.isna(date) or date in ['Null', 'Never had an election', 'Non-democracy']:
            return ''
        else:
            date = pd.to_datetime(date, errors='coerce')
            africa_wide_democracy_age = datetime.now().year - date.year
            return africa_wide_democracy_age

    # apply africa_wide_democracy_age function to columns
    countries_df['Years since first competitive election*'] = countries_df['Date that the first competitive democratic elections were held'].apply(africa_wide_democracy_age)
    countries_df['Age of current continuous democracy***'] = countries_df['Date that current continuous democracy started (i.e. elections were held)'].apply(africa_wide_democracy_age)

    # uninterrupted democracy and competitive elections columns
    countries_df['Uninterrupted democracy?**'] = np.where(countries_df['Years since first competitive election*'] == countries_df['Age of current continuous democracy***'], '✓ - Yes', 'No')
    countries_df['Currently holding competitive elections?****'] = np.where(countries_df['Age of current continuous democracy***'] != '', '✓ - Yes', 'No')

    # add info icon to countries with democracy age note 
    countries_df['Country name'] = countries_df.apply(lambda x: f"{x['Country']} &#9432; >>{x['Democracy age note']}" if pd.notna(x['Democracy age note']) else x['Country'], axis=1)

    # countries that have never had an election returns N/A for Uninterrupted democracy?* and No for Currently holding competitive elections?
    never_had_election = countries_df['Date that the first competitive democratic elections were held'].isin(['Never had an election', 'Null'])
    countries_df.loc[never_had_election, ['Uninterrupted democracy?*', 'Currently holding competitive elections?****']] = ['N/A', 'No']

    # ensure countries with blank or 0 in 'Age of current continuous democracy*' are marked correctly
    countries_df.loc[(countries_df['Age of current continuous democracy***'].isna()) | (countries_df['Age of current continuous democracy***'] == 0), 'Currently holding competitive elections?'] = 'No'

    # select columns to match the desired output
    africa_wide_democracy_age_df = countries_df[['Country name', 'Years since first competitive election*', 'Uninterrupted democracy?**', 'Age of current continuous democracy***', 'Currently holding competitive elections?****']]

    # sort the df alphabetically based on 'Country name'
    africa_wide_democracy_age_df = africa_wide_democracy_age_df.sort_values(by='Country name').reset_index(drop=True)

    # convert to int64 to remove decimals and return required columns for 'coup_df'
    gdp_df['GDP'] = gdp_df['GDP'].astype('Int64')
    population_df['Population'] = population_df['Population'].astype('Int64')
    coup_df = countries_df[['Country', 'State of Civilian Rule']]

    processed_dfs = {  # creates a dictionary with the desired df names
        'Democracy_Level': democracy_level_df,
        'Democracy_Age': countries_df[['Country', 'African Map Democracy Age']],
        'GDP': gdp_df,
        'Population': population_df,
        'Coup': coup_df,
        'africa_wide_democracy_age': africa_wide_democracy_age_df

    }

    def upload_africa_maps_to_s3():  # function to upload files to s3
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=bucket_name, Key=africa_maps_name, Body=csv_buffer.getvalue(),
                                 ContentType='text/csv')
            file_url = f"https://{bucket_name}.s3.amazonaws.com/{africa_maps_name}"
            print(f"File uploaded to {bucket_name}/{africa_maps_name}")
            list_of_all_s3_urls.append(file_url)
            return file_url
        except NoCredentialsError:
            print("Credentials not available")
        except Exception as e:
            print(f"Error: {str(e)}")
        return None

    file_names = {
        'Democracy_Level': 'africa-map-democracy-level.csv',
        'Democracy_Age': 'africa-map-democracy-age.csv',
        'GDP': 'africa-map-gdp.csv',
        'Population': 'africa-map-population.csv',
        'Coup': 'africa-map-coup.csv',
        'africa_wide_democracy_age': 'africa-wide-democracy-age.csv'

    }

    file_urls = {}  # upload the files to S3 and get the urls
    for key, df in processed_dfs.items():
        africa_maps_name = file_names[key]
        file_url = upload_africa_maps_to_s3()
        if file_url:
            file_urls[key] = file_url

    for key, url in file_urls.items():
        print(f'{key} URL: {url}')
        list_of_all_s3_urls.append(url)

    upload_africa_maps_to_s3()
    print('I am done!', 'generate_africa_maps')


@task
def generate_key_stats():
    global countries_df
    country_tables = {}  # creating transposed tables for countries with URL
    def classify_democracy_age(date):
        if date in ["Non-democracy"]:
            return date
        else:
            date = pd.to_datetime(date, format='%b-%y')
            current_year = datetime.now().year
            democracy_age = current_year - date.year
            return democracy_age

    countries_df['Key Stat Democracy Age'] = countries_df['Date that current continuous democracy started (i.e. elections were held)'].apply(classify_democracy_age)
    democracy_age_key_stat = countries_df[['Country', 'Key Stat Democracy Age']]

    def format_system_of_government(row):
        label = f"<b>{row['System of government label']}:</b>"
        columns = ['Who runs the government?', 'How are they elected?', 'Regional govts have autonomy?', 'Legislature?']
        bullet_points = [f"<li style='margin-left: 20px; margin-bottom: 2px;'>{row[col]}</li>" for col in columns if pd.notna(row[col])]
        
        if bullet_points:
            label += f"<ul style='margin-left: 20px; list-style-type: disc; padding-left: 20px;'>{''.join(bullet_points)}</ul>"
        return label

    countries_df['System of Government'] = countries_df.apply(format_system_of_government, axis=1)
    countries_stats = countries_df[['Country', 'System of Government']]

    # set dob and tenure column to date dtype
    countries_df['Current Pres Birth Date'] = pd.to_datetime(countries_df['Current Pres Birth Date'], format='%d-%b-%y')
    countries_df['Current Pres Start Date'] = pd.to_datetime(countries_df['Current Pres Start Date'], format='%d-%b-%y')

    # calculate current age of president
    def calculate_age(dob):
        today = datetime.today()
        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
        return age

    countries_df['President_age'] = countries_df['Current Pres Birth Date'].apply(calculate_age)

    # calculate tenure of president
    def calculate_tenure(in_office):
        today = datetime.today()
        tenure = today.year - in_office.year - ((today.month, today.day) < (in_office.month, in_office.day))
        return tenure

    countries_df['President_tenure'] = countries_df['Current Pres Start Date'].apply(calculate_tenure)

    countries_df['Age of Current President & Tenure'] = countries_df['President_age'].astype(str) + ' (' + \
                                                        countries_df['President_tenure'].astype(str) + '-yrs)'
    gdp_df['GDP'] = '$' + (gdp_df['GDP'] / 1000000000).round(1).astype(str) + 'bn'
    population_df['Population'] = (population_df['Population'] / 1000000).round(1).astype(str) + 'mn'

    merged_df = (
        population_df
        .merge(gdp_df, on='Country', how='left')
        .merge(countries_stats, on='Country', how='left')
        .merge(democracy_age_key_stat, on='Country', how='left')
        .merge(democracy_level_df, on='Country', how='left')
        .merge(countries_df, on='Country', how='left', suffixes=('', '_country'))
    )

    stats_table = merged_df[
        ['Country', 'Stears URL', 'Population', 'GDP', 'System of Government', 'Key Stat Democracy Age',
         'Democracy', 'Age of Current President & Tenure', 'State of Civilian Rule']]
    stats_table = stats_table.rename(columns={
        'Key Stat Democracy Age': 'Age of Democracy',
        'Democracy': 'Democracy Level',
        'State of Civilian Rule': 'Conflict/Coup Status'
    })
    stats_table.columns = [f'<b>{col}</b>' for col in stats_table.columns]  # adding bold tags to column names
    index = stats_table.columns.get_loc('<b>Democracy Level</b>')  # get the index for 'Democracy Level'
    stats_table.columns = [
        col + ' &#9432; >>EIU Democracy Index, 2024<br><br>The index is based on five categories: electoral process and pluralism, functioning of government, political participation, political culture, and civil liberties. Based on its 0-10 scores on a range of indicators within these categories, each country is classified as one of four types of regime: “full democracy”, “flawed democracy”, “hybrid regime” or “authoritarian regime."'
        if stats_table.columns.get_loc(col) == index else col for col in
        stats_table.columns]  # append the string to 'Democracy Level' column

    for index, row in stats_table.iterrows():  # creating transposed tables for countries
        country = row['<b>Country</b>']
        country_table = row.drop(['<b>Country</b>', '<b>Stears URL</b>']).to_frame(name='Value')
        country_table['Attribute'] = country_table.index
        country_table = country_table.reset_index(drop=True).reindex(columns=['Attribute', 'Value'])
        country_tables[country] = country_table

    def upload_keystats_to_s3():
        try:
            for country, df in country_tables.items():
                empty_row = pd.DataFrame([[''] * len(df.columns)], columns=df.columns)  # create an empty row
                final_table = pd.concat([empty_row, df], ignore_index=True)  # concatenate the empty row with the table

                csv_buffer = StringIO()
                final_table.to_csv(csv_buffer, index=False, header=False)

                s3_file_name = f'{country.lower().replace(" ", "-")}-key-stats.csv'
                s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=csv_buffer.getvalue(),
                                     ContentType='text/csv')

                print(f"{s3_file_name} uploaded to S3")
                print(f"https://{bucket_name}.s3.amazonaws.com/{s3_file_name}")
                list_of_all_s3_urls.append(f"https://{bucket_name}.s3.amazonaws.com/{s3_file_name}")
        except NoCredentialsError:
            print("Credentials not available")
            return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    upload_keystats_to_s3()
    print('All country tables uploaded successfully!')


@task
def generate_candidates():
    # Dictionary to hold dataframes from the current spreadsheet
    all_candidates_df = {}

    for country_name, country_id in country_name_fileid_data_dict.items():
        print(f'starting {country_name}')
        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)

        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        # Scrape google sheet into dataframes
        if 'Candidates' in spreadsheet.sheet_names:
            candidate_df = pd.read_excel(spreadsheet, sheet_name='Candidates')
            all_candidates_df[country_name] = candidate_df

            cols_to_keep = ['Source', 'Name', 'Headshot URL', 'Birth Date', 'Gender', 'Party', 'Coalition', 'Year',
                            'Previous Positions', 'Display', 'Winner']

            candidate_df = candidate_df.loc[:, cols_to_keep]

            split_positions = candidate_df['Previous Positions'].str.split(')', expand=True)
            split_positions.fillna('', inplace=True)
            new_previous_positions = split_positions.apply(lambda row: ')<br>'.join(row).rstrip(')<br>'), axis=1)

            candidate_df['new_previous_positions'] = new_previous_positions
            candidate_df['Coalition'] = candidate_df['Coalition'].replace('-', '')  
            candidate_df['new_previous_positions'] = candidate_df['new_previous_positions'].replace('\\n', '')  
            candidate_df['Text'] = ''

            for index,row in candidate_df.iterrows():
                # Example condition: if the 'age' column value is greater than 30
                if row['Coalition']:
                    # Modify the 'Text' column value
                    candidate_df.at[index, 'Text'] = (
                    '<br><b>Gender: </b><span style="color: white;">' + row['Gender'] + '</span> '
                    + '<br> <b>Party:</b> <span style="color: white;">' + row['Party'] + '</span>'
                    + '<br><b>Coalition:</b> <span style="color: white;">' + row['Coalition'] + '</span>'
                    + '<br><br><b>Previous Government Positions:</b>'
                    + '<br><span style="color: white;">' + row['new_previous_positions'] + ')</span>' )
                else:
                    candidate_df.at[index, 'Text'] = (
                    '<br><b>Gender: </b><span style="color: white;">' + row['Gender'] + '</span> '
                    + '<br> <b>Party:</b> <span style="color: white;">' + row['Party'] + '</span>'
                    + '<br><br><b>Previous Government Positions:</b>'
                    + '<br><span style="color: white;">' + row['new_previous_positions'] + ')</span>' )

            del candidate_df['new_previous_positions']

            for year in candidate_df['Year'].unique():
                # Filter the dataframe for the current year
                candidate_year_df = candidate_df[candidate_df['Year'] == year]

                def add_checkmark_to_winners(Name, Winner):
                    return Name + ' \u2713' if Winner == 'Yes' else Name

                candidate_year_df['Name'] = candidate_year_df.apply(
                    lambda row: add_checkmark_to_winners(row['Name'], row['Winner']), axis=1)

                # Only show candidates where Display is Yes
                indices_to_drop = candidate_year_df[candidate_year_df['Display'] != 'Yes'].index
                candidate_year_df = candidate_year_df.drop(indices_to_drop)



                # Process the save path - function to upload the manipulated dataframe to an S3 bucket
                def upload_candidates_to_s3():
                    # Convert DataFrame to CSV
                    csv_buffer = StringIO()
                    candidate_year_df.to_csv(csv_buffer, index=False)

                    # Generate the file name
                    candidate_file_name = f'{country_name}-candidates-{year}.csv'
                    print(f'https://{bucket_name}.s3.amazonaws.com/{candidate_file_name}')
                    list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{candidate_file_name}')
                    try:
                        # Upload the file
                        s3_client.put_object(Bucket=bucket_name, Key=candidate_file_name, Body=csv_buffer.getvalue(),
                                             ContentType='text/csv')
                        # print(f"{candidate_file_name} has been uploaded to {bucket_name}")
                    except NoCredentialsError:
                        print("Credentials not available")
                        return False
                    return True

                upload_candidates_to_s3()
        else:
            print('error: \'Candidates\' sheet not found')
    print('I am done!', 'generate_candidates')


@task
def generate_results_bar_charts():
    # Dictionary to hold dataframes from the current spreadsheet
    all_results_bar_charts_df = {}
    all_pres_results_bar_charts_df = {}

    for country_name, country_id in country_name_fileid_data_dict.items():
        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)

        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        def upload_dataframe_to_s3(df,bar_chart_file_name):
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            try:
                # Upload the file
                s3_client.put_object(Bucket=bucket_name, Key=bar_chart_file_name, Body=csv_buffer.getvalue(), ContentType='text/csv')
                print(f"{bar_chart_file_name} has been uploaded to {bucket_name}")
            except NoCredentialsError:
                print("Credentials not available")
                return False
            return True
        
        def process_pres_results_total():
            pres_results_total_bar_charts_df = pd.read_excel(spreadsheet, sheet_name='Pres-Results-Total')
            all_results_bar_charts_df[country_name] = pres_results_total_bar_charts_df

            pres_results_total_bar_charts_df['votes_sum'] = pres_results_total_bar_charts_df.iloc[:, 4:].sum(axis=1)
            pres_results_total_bar_charts_df.iloc[:, 4:-1] = pres_results_total_bar_charts_df.iloc[:, 4:-1].apply(
                lambda x: x / pres_results_total_bar_charts_df['votes_sum'] * 100, axis=0).round(2)
            pres_results_total_bar_charts_df.drop('votes_sum', axis=1, inplace=True)

            for year in pres_results_total_bar_charts_df['Year']:
                # Filter the pres_results_total_bar_charts_df for the current year
                pres_results_total_bar_charts_year_df = pres_results_total_bar_charts_df[pres_results_total_bar_charts_df['Year'] == year]

                if pres_results_total_bar_charts_year_df['Winning Party'].iloc[0] == 'Not available':
                    # Drop party columns for this dataframe specific to this year
                    pres_results_total_bar_charts_year_df.drop(pres_results_total_bar_charts_year_df.iloc[:, 4:], inplace=True, axis=1)
                    # Add a new column called 'Awaiting results'
                    pres_results_total_bar_charts_year_df.insert(4, 'Awaiting results', 100)
                else:
                    data_to_not_sort = pres_results_total_bar_charts_year_df.iloc[:, :4]  
                    data_to_sort = pres_results_total_bar_charts_year_df.iloc[:, 4:]
                    data = data_to_sort.transpose()
                    
                    data.sort_values(data.columns[0], ascending=False, inplace=True)

                    index_to_move_to_bottom = ['Other Parties']
                    row_to_move_to_bottom = data.loc[index_to_move_to_bottom]
                    row_to_not_move = data.drop(index_to_move_to_bottom)

                    new_df = pd.concat([row_to_not_move, row_to_move_to_bottom])
                    new_df = new_df.transpose()

                    pres_results_total_bar_charts_year_df = pd.concat([data_to_not_sort, new_df], axis=1)

                    print(f'{country_name} {year} results already known')

                pres_results_total_bar_charts_year_df.drop(columns=['Year', 'Winning Party'], inplace=True)

                # Generate the file name
                bar_chart_file_name = f'{country_name}-bar-{year}.csv'

                upload_dataframe_to_s3(pres_results_total_bar_charts_year_df,bar_chart_file_name)
                print(f'https://{bucket_name}.s3.amazonaws.com/{bar_chart_file_name}')
                list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{bar_chart_file_name}')
       
        def process_pres_election_results():
            pres_election_results_bar_charts_df = pd.read_excel(spreadsheet, sheet_name='Pres-Election-Results')
            all_pres_results_bar_charts_df[country_name] = pres_election_results_bar_charts_df

            pres_election_results_bar_charts_df['votes_sum'] = pres_election_results_bar_charts_df.iloc[:, 4:].sum(axis=1)
            pres_election_results_bar_charts_df.iloc[:, 4:-1] = pres_election_results_bar_charts_df.iloc[:, 4:-1].apply(
                lambda x: x / pres_election_results_bar_charts_df['votes_sum'] * 100, axis=0).round(2)
            pres_election_results_bar_charts_df.drop('votes_sum', axis=1, inplace=True)

            for year in pres_election_results_bar_charts_df['Year']:
                pres_election_results_bar_charts_year_df = pres_election_results_bar_charts_df[pres_election_results_bar_charts_df['Year'] == year]

                if pres_election_results_bar_charts_year_df['Winning Party'].iloc[0] == 'Not available':
                    # Drop party columns for this dataframe specific to this year
                    pres_election_results_bar_charts_year_df.drop(pres_election_results_bar_charts_year_df.iloc[:, 4:], inplace=True, axis=1)
                    # Add a new column called 'Awaiting results'
                    pres_election_results_bar_charts_year_df.insert(4, 'Awaiting results', 100)
                else:
                    print(f'{country_name} {year} results already known')

                pres_election_results_bar_charts_year_df.drop(columns=['Source', 'Year', 'Winning Party'], inplace=True)

                bar_chart_file_name = f'{country_name}-bar-{year}-Pres-Election-Results.csv'
                upload_dataframe_to_s3(pres_election_results_bar_charts_year_df, bar_chart_file_name)
                print(f'https://{bucket_name}.s3.amazonaws.com/{bar_chart_file_name}')
                list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{bar_chart_file_name}')

        if 'Pres-Results-Total' in spreadsheet.sheet_names and 'Pres-Election-Results' not in spreadsheet.sheet_names:
            process_pres_results_total()

        elif 'Pres-Results-Total' in spreadsheet.sheet_names and 'Pres-Election-Results' in spreadsheet.sheet_names:
            process_pres_results_total()
            process_pres_election_results()

    print('I am done!', 'generate_results_bar_charts')

@task    
def generate_results_maps():
    all_results_maps_df = {}

    for country_name, country_id in country_name_fileid_data_dict.items():
        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)

        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        # Scrape google sheet into dataframes
        if 'Pres-Results-Subnational' in spreadsheet.sheet_names:
            results_maps_df = pd.read_excel(spreadsheet, sheet_name='Pres-Results-Subnational')
            all_results_maps_df[country_name] = results_maps_df

            results_maps_df = results_maps_df.iloc[:, 2:]

            for year in results_maps_df['Year'].unique():
                results_maps_year_df = results_maps_df[results_maps_df['Year'] == year]
                results_maps_year_df.drop(['Year'], inplace=True, axis=1)

                # Identify float columns
                float_cols = results_maps_df.select_dtypes(include=['float64']).columns
                results_maps_df[float_cols] = results_maps_df[float_cols].fillna(0)

                # Convert the float columns to integers
                results_maps_df[float_cols] = results_maps_df[float_cols].astype(int)

                # print(results_maps_df.dtypes)
                # Process the save path - function to upload the manipulated dataframe to an S3 bucket
                def upload_dataframe_to_s3():
                    # Convert DataFrame to CSV
                    csv_buffer = StringIO()
                    results_maps_year_df.to_csv(csv_buffer, index=False)

                    # Generate the file name
                    results_maps_file_name = f'{country_name}-map-{year}.csv'

                    print(f'https://{bucket_name}.s3.amazonaws.com/{results_maps_file_name}')
                    list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{results_maps_file_name}')
                    try:
                        # Upload the file
                        s3_client.put_object(Bucket=bucket_name, Key=results_maps_file_name, Body=csv_buffer.getvalue(),
                                             ContentType='text/csv')
                        # print(f"{results_maps_file_name} has been uploaded to {bucket_name}")
                    except NoCredentialsError:
                        print("Credentials not available")
                        return False
                    return True

                upload_dataframe_to_s3()
    print('I am done!', 'generate_results_maps')


@task   
def generate_parliament_charts():
    # Dictionary to hold dataframes from the current spreadsheet
    all_parliament_charts_df = {}

    for country_name, country_id in country_name_fileid_data_dict.items():
        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)

        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        # Process the save path - function to upload the manipulated dataframe to an S3 bucket
        def upload_parliamentchart_to_s3(processed_df, file_name):
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            processed_df.to_csv(csv_buffer, index=False)

            try:
                # Upload the file
                s3_client.put_object(Bucket=bucket_name, Key=file_name,
                                        Body=csv_buffer.getvalue(), ContentType='text/csv')
                # print(f"{parliament_charts_file_name} has been uploaded to {bucket_name}")
            except NoCredentialsError:
                print("Credentials not available")
                return False
            return True

        # Scrape google sheet into dataframes
        if 'Legislative-Control' in spreadsheet.sheet_names:
            parliament_charts_df = pd.read_excel(spreadsheet, sheet_name='Legislative-Control')
            all_parliament_charts_df[country_name] = parliament_charts_df

            # Drop source & country columns
            parliament_charts_df = parliament_charts_df.iloc[:, 2:]

            def process_data(data):
                data = data.transpose()
                data = data.reset_index()

                # Replace NaNs with zero
                data = data.fillna(0)

                # Select only float columns
                float_columns = data.select_dtypes(include=['float64']).columns
                for col in float_columns:
                    data[col] = data[col].astype(int)

                data.columns = data.iloc[0]  # grab the first row for the header and set as header 
                data = data[1:]  # take the dataframe minus the header row
                data = data.reset_index(drop=True)
                
                data.rename(columns={"Year": "Coalition"}, inplace=True)
                data.sort_values(data.columns[1], ascending=False, inplace=True, ignore_index=True) 

                values_to_move_to_bottom = ['Other Parties', 'Appointed', 'Vacant', 'N/A - These seats did not exist at the time']
                indices_to_move_to_bottom=[]
                for value in values_to_move_to_bottom:
                    indices_to_move_to_bottom.extend(data.index[data['Coalition'] == value].tolist())

                rows_to_move_to_bottom = data.iloc[indices_to_move_to_bottom]
                rows_to_not_move = data.drop(indices_to_move_to_bottom)

                final_df = pd.concat([rows_to_not_move, rows_to_move_to_bottom], ignore_index=True)

                return final_df
            
            # Define the parliament types
            parliament_types = ['Bicameral', 'Unicameral', 'Upper', 'Lower']

            # Filter data for each year
            for year in parliament_charts_df['Year'].unique():
                # Filter the dataframe for the current year
                parliament_charts_year_df = parliament_charts_df[parliament_charts_df['Year'] == year]
            
                # Process each parliament type
                for p_type in parliament_types:
                    if any(parliament_charts_year_df['Parliament Type'] == p_type):

                        filtered_data = parliament_charts_year_df[parliament_charts_year_df['Parliament Type'] == p_type]
                        filtered_data.drop(columns='Parliament Type', inplace=True)

                        processed_data = process_data(filtered_data)
                        # processed_data.sort_values(processed_data.columns[1], ascending=False) 

                        # Generate the file name
                        parliament_charts_file_name = f'{country_name}-{p_type.lower()}-parliament-charts-{year}.csv'

                        upload_parliamentchart_to_s3(processed_data, parliament_charts_file_name)

                        print(f'https://{bucket_name}.s3.amazonaws.com/{parliament_charts_file_name}')
                        list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{parliament_charts_file_name}')
    print('I am done!', 'generate_parliament_charts')


@task
def generate_voter_metrics():
    # Dictionary to hold dataframes from the current spreadsheet
    all_voter_metrics_df = {}

    for country_name, country_id in country_name_fileid_data_dict.items():
        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)

        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        # Scrape google sheet into dataframes
        if 'Voter-Metrics' in spreadsheet.sheet_names:
            voter_metrics_df = pd.read_excel(spreadsheet, sheet_name='Voter-Metrics')
            all_voter_metrics_df[country_name] = voter_metrics_df

            # Drop columns not needed
            voter_metrics_df = voter_metrics_df.iloc[:, 0:14]

            # Select only float columns
            float_columns = voter_metrics_df.select_dtypes(include=['float64']).columns

            # Round values in float columns t`o 2 decimal places
            voter_metrics_df[float_columns] = voter_metrics_df[float_columns].round(2)

            # Process the save path - function to upload the manipulated dataframe to an S3 bucket
            def upload_votermetrics_to_s3():
                # Convert DataFrame to CSV
                csv_buffer = StringIO()
                voter_metrics_df.to_csv(csv_buffer, index=False)

                # Generate the file name
                voter_metrics_file_name = f'{country_name}-voter-metrics.csv'
                print(f'https://{bucket_name}.s3.amazonaws.com/{voter_metrics_file_name}')
                list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{voter_metrics_file_name}')
                try:
                    # Upload the file
                    s3_client.put_object(Bucket=bucket_name, Key=voter_metrics_file_name, Body=csv_buffer.getvalue(),
                                         ContentType='text/csv')
                    # print(f"{voter_metrics_file_name} has been uploaded to {bucket_name}")
                except NoCredentialsError:
                    print("Credentials not available")
                    return False
                return True

            upload_votermetrics_to_s3()
    print('I am done!', 'generate_voter_metrics')


def generate_election_resources():
    election_observer_directory_id = '1B1LyvUMhfrADMKYA4u7-sLp4tA0rBQcD'
    file_content = download_file_from_drive(election_observer_directory_id)

    # Load the Excel file into pandas
    spreadsheet = pd.ExcelFile(file_content)
    
    # Scrape google sheet into dataframes
    if 'Directory' in spreadsheet.sheet_names:
        directory_df = pd.read_excel(spreadsheet, sheet_name='Directory')

        directory_df = directory_df.iloc[:,:4]
        
        # Manipulate the 'Name' column using the 'Website' column
        directory_df['Name'] = directory_df.apply(lambda row: f'[{row["Name"]}]({row["Website"]})', axis=1)

        del directory_df['Website']

        # Process the save path - function to upload the manipulated dataframe to an S3 bucket
        def upload_election_resources_dataframe_to_s3():
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            directory_df.to_csv(csv_buffer, index=False)

            try:
                # Upload the file
                s3_client.put_object(Bucket=bucket_name, Key='election_resources.csv', Body=csv_buffer.getvalue(),
                                        ContentType='text/csv')
            except NoCredentialsError:
                print("Credentials not available")
                return False
            return True
        upload_election_resources_dataframe_to_s3()
        print(f"https://{bucket_name}.s3.amazonaws.com/election_resources.csv")
        list_of_all_s3_urls.append(f"https://{bucket_name}.s3.amazonaws.com/election_resources.csv")


@task
def generate_all_election_representativeness():

    # Dictionary to hold dataframes from the current spreadsheet
    all_election_representativeness_df = {}
    election_representativeness_list = []

    def upload_election_representativeness_table_to_s3(df):
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        try:
            # Upload the file
            s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue(),
                                    ContentType='text/csv')
            # print(f"{election_representativeness_file_name} has been uploaded to {bucket_name}")
        except NoCredentialsError:
            print("Credentials not available")
            return False
        return True

    for country_name, country_id in country_name_fileid_data_dict.items():
        print(f"Processing file for {country_name}")

        # Download the Excel file from Google Drive
        file_content = download_file_from_drive(country_id)
        # Load the Excel file into pandas
        spreadsheet = pd.ExcelFile(file_content)

        # Scrape google sheet into dataframes
        if 'Election-Representativeness' in spreadsheet.sheet_names:
            election_representativeness_df = pd.read_excel(spreadsheet, sheet_name='Election-Representativeness')
            all_election_representativeness_df[country_name] = election_representativeness_df

            # Filter data for each year
            for year in election_representativeness_df['Year'].unique():

                # Filter the dataframe for the current year
                election_representativeness_year_df = election_representativeness_df[election_representativeness_df['Year'] == year]
                election_representativeness_year_df = election_representativeness_year_df.iloc[:,4:7]
                election_representativeness_year_df.columns = [
                    'Did the results of the observation match the official results?',
                    'PP difference',
                    'Was the deviation enough to have changed the winner?']

                info_popup1 = ' ⓘ>>For the winning party, the percentage point difference in vote share between the PVT and official results was only ' 
                info_popup2 = 'pp'

                def update_text(Value, row):
                    if Value == 'Yes':
                        return '<font size="+2">' + ' \u2713 ' +  Value + '</font>' + info_popup1 + str(row['PP difference']) + info_popup2
                    else:
                        return '<font size="+2">' +  Value + '</font>'
                election_representativeness_year_df.iloc[:, 0] = election_representativeness_year_df.apply(lambda row: update_text(row.iloc[0], row), axis=1)
                

                def increase_font(Value):
                    return '<font size="+2">' +  Value + '</font>'
                election_representativeness_year_df.iloc[:, 2] = election_representativeness_year_df.iloc[:, 2].apply(increase_font)
     

                del election_representativeness_year_df['PP difference']

                # Generate the file name
                file_name = f'{country_name}-election-representativeness-{year}.csv'
                print(f'https://{bucket_name}.s3.amazonaws.com/{file_name}')
                list_of_all_s3_urls.append(f'https://{bucket_name}.s3.amazonaws.com/{file_name}')
            
                upload_election_representativeness_table_to_s3(election_representativeness_year_df)
            print('I am done! with uploading election_representativeness_table_to_s3 for each country\'s election year')
            
            # Renaming columns
            election_representativeness_df.rename(columns={
                'Country': 'Country',
                'Year': 'Year',
                'PVT: Was the winning party the same?': 'Did the results of the observation match the official results?',
                'PVT: Would the discrepancy have changed who won the overall election results?': 'Was the deviation enough to have changed the winner?',
                'PVT: For the winning party, what was the percentage point difference in vote share between PVT and official results?': 'How big was the deviation in % vote share for the winning party?'
            }, inplace=True)

            # format the 'Did the results of the observation match the official results?' column
            election_representativeness_df['Did the results of the observation match the official results?'] = election_representativeness_df[
                'Did the results of the observation match the official results?'
            ].apply(lambda x: '✓ Yes' if x == 'Yes' else 'No')

            # Format the 'How big was the deviation in % vote share for the winning party?' column
            election_representativeness_df['How big was the deviation in % vote share for the winning party?'] = election_representativeness_df[
                'How big was the deviation in % vote share for the winning party?'
            ].apply(lambda x: f"{x}pp")

            # create 'More details' column
            election_representativeness_df['More details'] = election_representativeness_df.apply(
                lambda row: f'View full reports from <a href="{row["Source"]}">{row["Observer Group"]}</a>',
                axis=1
            )

            # select and reorder columns
            election_representativeness_df = election_representativeness_df[[
                'Country',
                'Year',
                'Did the results of the observation match the official results?',
                'Was the deviation enough to have changed the winner?',
                'How big was the deviation in % vote share for the winning party?',
                'More details'
            ]]

            election_representativeness_list.append(election_representativeness_df)

    if election_representativeness_list:
        election_representativeness_df = pd.concat(election_representativeness_list, ignore_index=True)
        
        # sorting by country and year (descending)
        election_representativeness_df.sort_values(by=['Year'], ascending=[False], inplace=True)
        print("Data concatenation successful.")

        file_name = 'election-representativeness.csv'
        upload_election_representativeness_table_to_s3(election_representativeness_df)

        file_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
        list_of_all_s3_urls.append(f"https://{bucket_name}.s3.amazonaws.com/{file_name}")
        print(f"File uploaded to {bucket_name}/{file_name}")
        return file_url


def generate_term_limits():
    def process_term_limits():
        global term_limits_df

        term_limits_df['Sequence'] = term_limits_df.groupby('Country').cumcount() + 1

        # Create the 'Presidential Sequence' column
        suffixes = {1: 'st', 2: 'nd', 3: 'rd'}

        def get_sequence(n):
            if 10 <= n % 30 <= 20:
                suffix = 'th'
            else:
                suffix = suffixes.get(n % 10, 'th')
            return f"{n}{suffix} Leader"

        term_limits_df['Presidential Sequence'] = term_limits_df['Sequence'].apply(get_sequence)

        # Renaming columns
        term_limits_df.rename(columns={
            'Number of terms served': 'Terms served',
            'Term limit': 'Legal maximum number of terms',
            'Term length': 'Legal maximum duration of each term',
            'Historical Context': 'Historical compliance'
        }, inplace=True)

        # Define the current year
        current_year = datetime.now().year

        # Create a temporary column for calculation purposes
        term_limits_df['End Year Temporary'] = term_limits_df['End Year'].replace('Incumbent', current_year)
        term_limits_df['End Year Temporary'] = pd.to_numeric(term_limits_df['End Year Temporary'], errors='coerce')  # Convert to numeric, coerce errors to NaN

        # Create the 'Duration' column
        term_limits_df['Duration'] = term_limits_df.apply(
            lambda row: '' if pd.isna(row['End Year Temporary']) else int(row['End Year Temporary']) - row['Start Year'],
            axis=1
        )

        # Drop the temporary column
        term_limits_df = term_limits_df.drop(columns=['End Year Temporary'])

        # Create the 'Sizing' column and Country-A
        term_limits_df['Country-A'] = term_limits_df['Country']
        term_limits_df['Sizing'] = term_limits_df.apply(
            lambda row: '' if row['Duration'] == '' else row['Duration'] ** 2, axis=1
        )

        # Fill NaN values with a default value or handle them as required
        term_limits_df['Start Year'] = term_limits_df['Start Year'].fillna(0).astype(int)
        term_limits_df['Duration'] = term_limits_df['Duration'].apply(lambda x: int(x) if x != '' else 0)
        term_limits_df['Sizing'] = term_limits_df['Sizing'].apply(lambda x: int(x) if x != '' else 0)

        # sort by country
        term_limits_df.sort_values(by=['Country', 'Country-A', 'Sequence'], ascending=[True, True, True], inplace=True)

        # Select and reorder columns
        term_limits_df = term_limits_df[[
            'Country',
            'Sequence',
            'President name',
            'Presidential Sequence',
            'Status',
            'Start Year',
            'End Year',
            'Duration',
            'Terms served',
            'Legal maximum number of terms',
            'Legal maximum duration of each term',
            'Historical compliance',
            'Sizing',
            'Country-A'
        ]]

        return term_limits_df

    def upload_term_limits_to_s3():  # load to s3
        try:
            term_limits_name = 'term_limits.csv'
            csv_buffer = StringIO()
            term_limits_df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=bucket_name, Key=term_limits_name, Body=csv_buffer.getvalue(),
                                 ContentType='text/csv')
            file_url = f"https://{bucket_name}.s3.amazonaws.com/{term_limits_name}"
            
            print(f"File uploaded to {bucket_name}/{term_limits_name}")
            return file_url
        except NoCredentialsError:
            print("Credentials not available")
            return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    term_limits_df = process_term_limits()
    file_url = upload_term_limits_to_s3()
    print(f'File URL: {file_url}')
    list_of_all_s3_urls.append(file_url)
    print('I am done!', 'generate_term_limits')


@flow(retries=3, retry_delay_seconds=5, log_prints=True)
def refresh_election_data():
    setup_is_successful = setup()
    if setup_is_successful:
        generate_both_trackers()
        generate_upcoming_points()
        generate_africa_maps()
        generate_key_stats()
        generate_candidates()
        generate_results_bar_charts()
        generate_results_maps()
        generate_parliament_charts()
        generate_voter_metrics()
        generate_election_resources()
        generate_all_election_representativeness()
        generate_term_limits()
        print('Here are all the URLs:')
        print(list_of_all_s3_urls)
    else:
        raise Exception()


refresh_election_data_deployment = refresh_election_data.to_deployment(name='Open: Refresh election data deployment',
                                                                       cron='0 23 * * *')

if __name__ == "__main__":
    refresh_election_data() # Run this to see if the code works, all the functions are called under 'refresh_election_data' so they aren't called earlier
