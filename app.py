import streamlit as st
import pandas as pd
import requests
import json
import time
import datetime

st.write('hello')

username = st.text_input('Username')
password = st.text_input('Password', type='password')

uploaded_data = st.file_uploader('Input csv file with domains list (websites column has to be named "websites")')
if uploaded_data is not None:
    domains_to_check_df = pd.read_csv(uploaded_data)


urls_list = []
websites_text_area = st.text_area('Input list of websites (separeted by newline)')
if uploaded_data is not None:
    urls_list = domains_to_check_df['Website'].values.tolist()
    st.write(f'Number of websites: {len(urls_list)}')
elif websites_text_area != '':
    websites_to_check = websites_text_area.split('\n')
    urls_list = [website for website in websites_to_check]
    st.write(f'Number of websites: {len(urls_list)}')


keywords_to_check_list = []
keywords_to_check = st.text_area('Input keywords (separeted by newline)')
if keywords_to_check != '':
    keywords_to_check = keywords_to_check.split('\n')
    keywords_to_check_list = [keyword for keyword in keywords_to_check]
    st.write(f'number of keywords: {len(keywords_to_check_list)}')


def generate_list_of_queries(keywords_list, urls_list):
    queries_list = []
    for keyword in keywords_list:
        for website in urls_list:
            query = f'site:{website} intext:{keyword}'
            queries_list.append(query)

    queries_list_devided = [queries_list[x:x+1000] for x in range(0, len(queries_list), 1000)]

    post_request_list = []
    for list_of_queries in queries_list_devided:
        keywords_json = {}
        keywords_json['query'] = list_of_queries
        keywords_json['source'] = 'google_search'
        keywords_json['domain'] = 'com'
        keywords_json['parse'] = 'true'
        post_request_list.append(keywords_json)

    return post_request_list


def send_post_request(query):
    post_request_data = {}
    response = requests.request('POST', 'https://data.oxylabs.io/v1/queries/batch', auth=(username, password), json=query,)
    for query in response.json()['queries']:
        post_request_data[query['query']] = [query['id']]

    return post_request_data


def get_job_data(post_request_data):
    data = {}
    for query in post_request_data:
        query_id = post_request_data[query][0]
        response = requests.request(method='GET', url=f'http://data.oxylabs.io/v1/queries/{query_id}/results', auth=(username, password),)
        res = response.json()['results']
        data[query] = res

    return data


def parse_data(jobs_data):
    data = []
    for res in jobs_data:

        website = res.split(' ', 1)[0].replace('site:', '')
        keyword = res.split(' ', 1)[1].replace('intext:', '')

        if 'organic' in jobs_data[res][0]['content']['results'].keys():
            organic_results = jobs_data[res][0]['content']['results']['organic']

            if len(organic_results) > 0:
                for result in organic_results:
                    row = []
                    row.append(website)
                    row.append(keyword)
                    row.append('All good')
                    row.append(result['url'])
                    row.append(result['title'])
                    row.append(result['desc'])

                    data.append(row)
            else:
                pass
        else:
            pass

    return data


def main():
    queries_list = generate_list_of_queries(keywords_to_check_list, urls_list)
    df_data = []
    for query in queries_list:
        post_request_data = send_post_request(query)
        time.sleep(60)
        jobs_data = get_job_data(post_request_data)
        parsed_data = parse_data(jobs_data)
        df = pd.DataFrame(parsed_data, columns = ['website', 'keyword', 'status', 'url', 'title', 'description'])
        df_data.append(df)

    df_final = pd.DataFrame(columns=['website', 'keyword', 'status', 'url', 'title', 'description'])
    for df in df_data:
        df_final = pd.concat([df_final, df])

    filtered_data = []
    for row in df_final.iterrows():
        keyword = row[1]['keyword']
        desc = row[1]['description']
        if keyword in desc.lower():
            filtered_data.append(row[1])

    df_final_final = pd.DataFrame(filtered_data, columns = ['website', 'keyword', 'status', 'url', 'title', 'description'])

    return df_final_final


# Streamlit code
if username != '' and password != '' and urls_list != [] and keywords_to_check_list != []:
    if st.button('Start the script'):
        st.write('Starting the script')
        df_final_final = main()
        st.download_button(
     label="Download data as CSV",
     data=df_final_final.to_csv(),
     file_name='large_df.csv',
     mime='text/csv',)
