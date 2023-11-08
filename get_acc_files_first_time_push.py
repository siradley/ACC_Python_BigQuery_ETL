import io
import os
import gc
import json
import time
import requests
import pandas as pd
import concurrent.futures

gc.collect()

accessToken = "accessToken_here"
projectId = "projectId_here"
folderId = "folderId_here"

def main():
    start_time = time.time()

    collected_files_list = get_acc_files_by_search(projectId, folderId)
    print("get_acc_files_by_search COMPLETED")

    array_of_itemId = create_itemId_list(collected_files_list)
    print("create_itemId_list COMPLETED")

    array_of_itemId_for_filePath = array_of_itemId['array_of_itemId']
    pathInProject = get_filePath_by_itemid(projectId, array_of_itemId_for_filePath)
    print("get_filePath_by_itemid COMPLETED")

    extractPath = pathInProject['pathInProject']
    csv_output = output_as_csv(collected_files_list, extractPath)
    print("output_as_csv COMPLETED")

    # with open('collected_files_list.json', 'w') as json_file:
    #     json.dump(collected_files_list, json_file, indent=4)
    # with open('pathInProject.json', 'w') as json_file:
    #     json.dump(pathInProject, json_file, indent=4)

    end_time = time.time()
    execution_time = end_time - start_time

    print('File outputted successfully!')
    print(f"Execution time: {execution_time} seconds")

def get_acc_files_by_search(projectId, folderId):
    collected_files_list = []

    url = f"https://developer.api.autodesk.com/data/v1/projects/{projectId}/folders/{folderId}/search"
    headers = {"Authorization": f"Bearer {accessToken}"}

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
    return collected_files_list

def create_itemId_list(collected_files_list):
    id_list = []

    for entry in collected_files_list:
        data = entry.get("data", [])
        for item in data:
            relationships = item.get("relationships", {})
            item_data = relationships.get("item", {}).get("data", {})
            item_id = item_data.get("id", "")
            if item_id:
                id_list.append(item_id)

    return {'array_of_itemId': id_list}

def get_filePath_by_itemid(projectId, array_of_itemId_for_filePath):
    pathInProject = []

    base_url = f"https://developer.api.autodesk.com/data/v1/projects/{projectId}"
    headers = {"Authorization": f"Bearer {accessToken}"}

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

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(fetch_item_path, array_of_itemId_for_filePath)

    return {'pathInProject': pathInProject}

def flatten_json(json_obj, parent_key='', separator='_', ignore_arrays=True):
    flattened = {}
    for key, value in json_obj.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            # Recursively flatten the nested dictionary
            flattened.update(flatten_json(value, new_key, separator, ignore_arrays))
        elif isinstance(value, list) and not ignore_arrays:
            # Flatten the list if ignore_arrays is False
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