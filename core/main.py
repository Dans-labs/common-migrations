# Common migration pipeline for Dataverse developed by DANS-KNAW R&D group
# Authors:
# Eko Indarto (DCCD project)
# Vic Ding (ODISSEI project)
# Slava Tykhonov (CLARIAH project)

from datetime import datetime

import jinja2
import json
import logging
import os
import pandas as pd
import requests
from jproperties import Properties

import config


def setup():
    global file_errors_records, file_exported_datasets_list, headers_file, templateLoader, templateEnv, headers, \
        dataset_json_template, file_json_template, configs_dict
    timestamp_str = '_' + datetime.now().strftime("%Y%m%d_%H%M%S")
    file_errors_records = open(config.FILE_CSV_ERRORS_RECORDS + timestamp_str + '.csv', 'a')
    file_exported_datasets_list = open(config.FILE_EXPORTED_RECORDS + timestamp_str + '.txt', 'a')
    headers = {'X-Dataverse-key': config.DATAVERSE_API_TOKEN}
    headers_file = {
        'X-Dataverse-key': config.DATAVERSE_API_TOKEN,
    }
    templateLoader = jinja2.FileSystemLoader(searchpath="../resources/templates")
    templateEnv = jinja2.Environment(loader=templateLoader)
    dataset_json_template = templateEnv.get_template(config.DATASET_JSON_TEMPLATE)
    file_json_template = templateEnv.get_template(config.FILE_JSON_TEMPLATE)
    configs = Properties()
    with open(config.JINJA_CSV_MAPPING_FILE, 'rb') as read_prop:
        configs.load(read_prop)
    items_view = configs.items()
    configs_dict = {}
    for item in items_view:
        configs_dict[item[0]] = item[1].data
    print(configs_dict)


def read_csv():
    df = pd.read_csv(config.CSV_FILE_INPUT).rename(columns=configs_dict)
    print(df.index)
    csv_records = df.to_dict(orient='records')
    for row in csv_records:
        dv_dataset_json = dataset_json_template.render(row)
        sid = row['sid']
        organization_name = row['organization_name']
        valid_json = validate_json(dv_dataset_json)
        # print(dv_dataset_json)
        if not valid_json:
            print(valid_json)
            print(dv_dataset_json)
            exit()
            file_errors_records.write(sid)
            file_errors_records.write('\n')

        else:
            if config.JSON_VALIDATION_ONLY:
                print("valid")
                persistent_id = 'FAKE-' + sid
                ingest_files(file_json_template, organization_name, persistent_id, sid, True)
                file_exported_datasets_list.write(persistent_id)
                file_exported_datasets_list.write('\n')
            else:
                dv_resp = requests.post(
                    config.DATAVERSE_BASE_URL + '/api/dataverses/' + config.DATAVERSE_TARGET + '/datasets',
                    data=dv_dataset_json.encode('utf-8'), headers=headers)
                # logging.info("END ingest for " + sid)
                print(dv_resp.json())
                if dv_resp and dv_resp.status_code == 201:
                    print('Ingest successed...')
                    persistent_id = dv_resp.json()['data']['persistentId']
                    id = str(dv_resp.json()['data']['id'])
                    logging.info("pid: " + persistent_id + " id: " + id)
                    ingest_files(file_json_template, organization_name, persistent_id, sid, False)
                    if config.INGEST_AND_DELETE:
                        delete_status = delete_draft_dataset(id)
                        print("delete status: " + delete_status)
                    file_exported_datasets_list.write(sid + '|' + persistent_id + '|' + id)
                    file_exported_datasets_list.write('\n')
                else:
                    logging.error(">>>>>> ERROR for pid:" + sid)
                    logging.error(">>>>>> ERROR dv_resp: " + str(dv_resp))
                    print("ERROR with dv_resp: " + str(dv_resp))
                    if dv_resp.status_code == 401:
                        exit()

    file_errors_records.close()
    if is_file_empty(file_errors_records.name):
        os.remove(file_errors_records.name)

    file_exported_datasets_list.close()
    if is_file_empty(file_exported_datasets_list.name):
        os.remove(file_exported_datasets_list.name)
    print("end")


# Check whether a file is empty or not.
def is_file_empty(file_path):
    """ Check if file is empty by confirming if its size is 0 bytes"""
    # Check if file exist and it is empty
    return os.path.exists(file_path) and os.stat(file_path).st_size == 0


def ingest_files(file_json_template, organization, persistent_id, sid, dry_run):
    dir_dccd = sid.replace(':', '_')
    tridas_file_json = file_json_template.render(directory_label='')
    # json validation for tridas.xml metadata. Todo: We need to validate all files, not only tridas.xml
    valid_tridas_json = validate_json(tridas_file_json)
    if valid_tridas_json:
        add_file(persistent_id, 'tridas.xml',
                 os.path.join(config.BASE_PATH_INGEST_FILES, organization, dir_dccd, 'tridas.xml')
                 , tridas_file_json, dry_run)
        asscociated_files = get_associated_files(organization, dir_dccd)
        for associated_file in asscociated_files:
            associated_file_json = file_json_template.render(directory_label=associated_file['directoryLabel'])
            add_file(persistent_id, associated_file['file_name'], associated_file['absolut_path']
                     , associated_file_json, dry_run)
        originalvalues_files = get_originalvalues_files(organization, dir_dccd)
        for originalvalues_file in originalvalues_files:
            originalvalues_file_json = file_json_template.render(directory_label=originalvalues_file['directoryLabel'])
            add_file(persistent_id, originalvalues_file['file_name'], originalvalues_file['absolut_path'],
                     originalvalues_file_json, dry_run)


def validate_json(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True


def add_file(pid, file_name, absolute_path, file_metadata, dry_run):
    if dry_run:
        return
    params = (
        ('persistentId', pid),
    )

    files = {
        'file': (file_name, open(absolute_path, 'rb')),
        'jsonData': (None, file_metadata),
    }
    response = requests.post(config.DATAVERSE_BASE_URL + '/api/datasets/:persistentId/add', headers=headers_file,
                             params=params, files=files)
    # print(response.json())
    return response.status_code


def delete_draft_dataset(id):
    response = requests.delete(config.DATAVERSE_BASE_URL + '/api/datasets/' + id + '/versions/:draft',
                               headers=headers_file)
    if response and response.status_code == 200:
        return response.json()['data']['message']
    else:
        print(response.json())


def get_originalvalues_files(organization_name, dir_dccd):
    return get_files(os.path.join(config.BASE_PATH_INGEST_FILES, organization_name, dir_dccd), 'originalvalues')


def get_associated_files(organization_name, dir_dccd):
    return get_files(os.path.join(config.BASE_PATH_INGEST_FILES, organization_name, dir_dccd), 'associated')


def get_files(dir_start, sub_dir):
    dir_path = os.path.join(dir_start, sub_dir)
    file_metadatas = []
    for root, dirs, files in os.walk(dir_path):
        # print(root)
        # print(dirs)
        for file in files:
            if os.path.basename(file) != '.DS_Store':
                file_metadata = {'file_name': file, 'absolut_path': os.path.join(root, file),
                                 'directoryLabel': root.replace(dir_start + '/', '')}
                file_metadatas.append(file_metadata)
    return file_metadatas


if __name__ == "__main__":
    setup()
    start_time = datetime.now()
    logging.basicConfig(filename='../logs/import.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

    # Todo: log location
    # Todo: Put directory/file checking in a method
    # Check if BASE_PATH_INGEST_FILES exist
    if not os.path.isdir(config.BASE_PATH_INGEST_FILES):
        msg = "'" + config.BASE_PATH_INGEST_FILES + "' directory doesn't exist."
        logging.error(msg)
        print(msg)
        exit()
    # Check if CSV_FILE exist
    if not os.path.isfile(config.CSV_FILE_INPUT):
        msg = "'" + config.CSV_FILE_INPUT + "' csv input file doesn't exist."
        logging.error(msg)
        print(msg)
        exit()

    setup()
    read_csv()
    end_time = datetime.now()
    msg = 'Duration: {}'.format(end_time - start_time);
    print(msg)
    logging.info(msg)
