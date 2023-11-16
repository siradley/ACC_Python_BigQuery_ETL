import io
import os
import gc
import json
import time
import requests
import pandas as pd
import concurrent.futures

gc.collect()

accessToken = "<AccessToken>"
project_id = "<projectId>"
folder_id = "<folderId>"

def main():
    start_time = time.time()

    responses = get_top_folder_id(project_id, folder_id)
    folder_ids = [folder.get("id") for folder in responses.get("data", [])]

    all_collected_files_list = get_acc_files_by_search(project_id, folder_ids)
    print("get_acc_files_by_search COMPLETE")

    #temp
    with open('collected_files_list.json', 'w') as json_file:
        json.dump(all_collected_files_list, json_file, indent=4)
    getLength = len(all_collected_files_list)
    print(getLength)

    array_of_itemId = create_itemId_list(all_collected_files_list)
    print("create_itemId_list COMPLETE")

    array_of_itemId_for_filePath = array_of_itemId['array_of_itemId']
    pathInProject = get_filePath_by_itemid(project_id, array_of_itemId_for_filePath)
    print("get_filePath_by_itemid COMPLETE")

    extractPath = pathInProject['pathInProject']
    csv_output = output_as_csv(all_collected_files_list, extractPath)
    print("output_as_csv COMPLETE")

    end_time = time.time()
    execution_time = end_time - start_time

    print('FILE OUTPUTTED SUCCESSFULLY!')
    print(f"Execution time: {execution_time} seconds")

def get_top_folder_id(project_id, folder_id):
    url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/contents"
    headers = {"Authorization": f"Bearer {accessToken}"}
    params = {"filter[type]": "folders"}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        responses = response.json()
    else:
        print("hello world")

    return responses

def get_acc_files_by_search(project_id, folder_ids):
    all_collected_files_list = []

    headers = {"Authorization": f"Bearer {accessToken}"}

    for folder_id in folder_ids:
        url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}/folders/{folder_id}/search"
        collected_files_list = []

        while True:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                response_json = response.json()
                collected_files_list.append(response_json)
                next_url = response_json.get("links", {}).get("next", {}).get("href")
                if next_url:
                   url = next_url
                else:
                    break
            else:
                break

        all_collected_files_list.extend(collected_files_list)

    return all_collected_files_list

def create_itemId_list(all_collected_files_list):
    id_list = []

    for entry in all_collected_files_list:
        data = entry.get("data", [])

        for item in data:
            relationships = item.get("relationships", {})
            item_data = relationships.get("item", {}).get("data", {})
            item_id = item_data.get("id", "")
            if item_id:
                id_list.append(item_id)

    return {'array_of_itemId': id_list}

def get_filePath_by_itemid(project_id, array_of_itemId_for_filePath):
    pathInProject = []

    file_path_one = "accessToken.txt"
    with open(file_path_one, "r") as file_one:
        diff_access_token = file_one.read().strip()

    base_url = f"https://developer.api.autodesk.com/data/v1/projects/{project_id}"
    headers = {"Authorization": f"Bearer {diff_access_token}"}

    def fetch_item_path(itemId):
        get_item_by_id_url = f"{base_url}/items/{itemId}"
        request_body = {"includePathInProject": "true"}
        response = requests.get(get_item_by_id_url, headers=headers, params=request_body)

        if response.status_code == 200:
            response_json = response.json()
            path_in_project = response_json.get('data', {}).get('attributes', {}).get('pathInProject')
            pathInProject.append(path_in_project)
        else:
            print(f"Error for item {itemId}: {response.status_code}")
            print('retry')
            file_path = "sec_accessToken.txt"
            with open(file_path, "r") as file:
                next_access_token = file.read().strip()
            newHeaders = {"Authorization": f"Bearer {next_access_token}"}
            newResponse = requests.get(get_item_by_id_url, headers=newHeaders, params=request_body)
            if newResponse.status_code == 200:
                print("nice.")
                newResponse_json = newResponse.json()
                path_in_project = newResponse_json.get('data', {}).get('attributes', {}).get('pathInProject')
                pathInProject.append(path_in_project)
            else:
                print('still cannot. why :((')

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(fetch_item_path, array_of_itemId_for_filePath)

    return {'pathInProject': pathInProject}

def flatten_json(json_obj, parent_key='', separator='_', ignore_arrays=True):
    flattened = {}
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key, separator, ignore_arrays))
        elif isinstance(value, list) and not ignore_arrays:
            for i, item in enumerate(value, start=1):
                sub_key = f"{new_key}{separator}{i}"
                flattened[sub_key] = item
        else:
            flattened[new_key] = value
    return flattened

def output_as_csv(collected_files_list, extractPath):
    df_one = pd.DataFrame()
    df_two = pd.DataFrame({"filePath": extractPath})

    output_file = "LIST_OF_FILES_WITH_PATH.csv"

    for entry in collected_files_list:
        flat_data = flatten_json(entry, ignore_arrays=True)
        df_new = pd.DataFrame(flat_data)
        df_one = pd.concat([df_one, df_new], ignore_index=True)

    for index, row in df_one.iterrows():
        data = flatten_json(row['data'], ignore_arrays=True)
        for key, value in zip(data, data.values()):
            key = "data_" + key
            df_one.at[index, key] = value
        included = flatten_json(row['included'], ignore_arrays=True)
        for key, value in zip(included, included.values()):
            key = "included_" + key
            df_one.at[index, key] = value

    data = df_one.to_csv(index=False)
    lines = data.strip().split('\n')
    df_left = pd.read_csv(io.StringIO('\n'.join(lines)))
    combined_df = df_left.join(df_two)
    combined_df.to_csv(output_file, index=False)

    return combined_df

main()


# def get_custom_attributes(project_id, array_of_itemId_for_filePath):
#     custom_attributes_list = []
#
#     get_custom_attribute_url = f"https://developer.api.autodesk.com/bim360/docs/v1/projects/{project_id}/versions:batch-get"
#     headers = {
#         "Authorization": f"Bearer {accessToken}",
#         "Content-Type": "application/json"
#     }
#
#     def fetch_custom_attributes(itemId):
#         request_body = {"urns": [
#             itemId
#         ]}
#         response = requests.get(get_custom_attribute_url, headers=headers, params=request_body)
#
#         if response.status_code == 200:
#             response_json = response.json()
#             custom_attributes = response_json["results"][0].get("customAttributes", [])
#             custom_attributes_list.append(custom_attributes)
#         else:
#             print(f"Error for item {itemId}: {response.status_code}")
#
#     with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
#         executor.map(fetch_custom_attributes, array_of_itemId_for_filePath)
#
#     return {'custom_attributes_list': custom_attributes_list}