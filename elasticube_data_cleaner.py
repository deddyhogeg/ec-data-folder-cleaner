# -*- coding: utf-8 -*-
'''
Python 3.7.3
Author : David Hogeg <david.hogeg@sisense.com>
This Script was Written to clean ElastiCubeData folder

Version 3.0
-----------
Author : David Hogeg <david.hogeg@sisense.com>
'''
import asyncio
import time
import datetime
import psutil
import shutil
# from datetime import datetime
import requests
import logging
import json
import sys
import os
import glob
from xml.etree.ElementTree import parse
import shutil  # zip files
import argparse
import zipfile
import yaml
import pprint
import queue
import subprocess
import re
import traceback

if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
elif __file__:
    application_path = sys.path[0]

# logger = logging.getLogger(__name__)
logger = logging.getLogger("asyncio")

# Init global vars
START = time.time()
APP_DIR = "C:\ProgramData\Sisense\PrismServer\ElastiCubeData_Cleaner"
ELASTICUBE_DEFAULT_DATA_FOLDER = "C:\ProgramData\Sisense\PrismServer\ElastiCubeData"
ELASTICUBE_DATA_FOLDERS = []
PSM_DEFAULT_PATH = "C:\\Program Files\\Sisense\\Prism\\Psm.exe"
DEFAULT_SERVER_ADDRESS = "localhost"
ACTION = "list"
DEFAULT_EC_SERVICE_NAME = "Sisense.ECMS"
EC_SERVICE_NAME = DEFAULT_EC_SERVICE_NAME
LOG_DATE = str(datetime.date.today())
# DEFAULT_LOG_FILE = "C:\\ProgramData\\Sisense\\PrismServer\\ElastiCubeData_Cleaner\\logs\\"

# Create new APP_DIR folder for the logs if doesn't exist
if not os.path.exists(APP_DIR):
    os.makedirs(APP_DIR)
    os.makedirs(APP_DIR + '\logs')


async def get_active_elasticubes_list():
    os.environ['SISENSE_PSM'] = 'True'
    # PSM output structure: Cube Name [NW_A] ID : [aNWXwAaA] FarmPath [C:\ProgramData\SiSense\PrismServer\ElastiCubeData\aNWXwAaA_2016-11-15_23-14-59] Status [STOPPED]
    p_output = subprocess.check_output(
        [PSM_DEFAULT_PATH, 'ecs', 'ListCubes', 'serverAddress=' + DEFAULT_SERVER_ADDRESS])

    split_output = p_output.splitlines()

    # regex to extract the the cube info from the psm command output
    re_comp = re.compile(
        "^Cube Name \[(?P<name>.*)\] ID *.\s\[(?P<cube_id>.*)\] FarmPath \[(?P<dbfarm_path>.*)\] Status \[(?P<cube_status>.*)\]$")

    ec_list = []

    for output_str in split_output:

        # output_str = ((str(output_str)).encode('utf-8', 'ignore'))

        m = re_comp.search(str(output_str, 'utf-8', 'ignore'))

        if m is not None:
            ec_list.append(m.group('name'))

    logger.debug(ec_list)
    return ec_list


async def get_active_elasticube_info(ec_name):
    p_output = subprocess.check_output(
        [PSM_DEFAULT_PATH, 'ecube', 'info', 'name=' + ec_name, 'serverAddress=' + DEFAULT_SERVER_ADDRESS])

    split_output = p_output.splitlines()

    # regex to extract the the cube info from the psm command output
    re_comp = re.compile("(?P<attr>^.*): (?P<val>.*$)")
    # re_comp = re.compile(
    #     "^(?P<attr>^\b[a-zA-Z0-9]+\b:) (?P<val>.*$)")

    ec_info = dict()

    for output_str in split_output:

        # output_str = ((str(output_str)).encode('utf-8', 'ignore'))
        # print(output_str)
        m = re_comp.search(str(output_str, 'utf-8', 'ignore'))

        if m is not None:
            ec_info[m.group('attr')] = m.group('val')
    # pp = pprint.PrettyPrinter(indent=6)
    # pp.pprint(ec_info)
    # logger.debug(ec_info)
    return ec_info


async def get_ec_data_folders(ec_id, ec_db_folder):
    paths = []
    # folder_list = os.scandir(ELASTICUBE_DATA_FOLDER)
    # logger.debug(os.path.dirname(ec_db_folder))
    if os.path.dirname(ec_db_folder) not in ELASTICUBE_DATA_FOLDERS:
        ELASTICUBE_DATA_FOLDERS.append(os.path.dirname(ec_db_folder))

    for folder in ELASTICUBE_DATA_FOLDERS:
        with os.scandir(folder) as it:
            for entry in it:
                if entry.is_dir:

                    if os.path.exists(os.path.join(entry.path, "dbfarm")):
                        with os.scandir(os.path.join(entry.path, "dbfarm")) as it2:

                            for entry2 in it2:
                                if entry2.name == ec_id:

                                    ret = is_path_in_list(entry.path, paths)
                                    if not ret:
                                        paths.append(entry.path)

    return paths


def init_logging(logger, fileLogLevel='DEBUG', consoleLogLevel='INFO'):
    # logger = logging.getLogger(__name__)
    logger.setLevel(fileLogLevel)

    # Create file handler
    log_file_name = 'clean_' + LOG_DATE + '.log'
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(fileLogLevel)

    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(consoleLogLevel)

    # Create formatter for log file messages
    f_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    #  Create formatter for console messages
    # c_formatter = logger.Formatter('%(levelname)s - %(message)s')
    c_formatter = logging.Formatter('%(message)s')

    fh.setFormatter(f_formatter)

    ch.setFormatter(c_formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


def get_args():
    """Get CLI arguments and options"""

    parser = argparse.ArgumentParser(description='Clean up elasticube data folder.')
    parser.add_argument('action', choices=['list', 'delete'])
    parser.add_argument('--ec_data_folders', help="The ElasricubeDataFolders", nargs='+',
                        default=[ELASTICUBE_DEFAULT_DATA_FOLDER])
    parser.add_argument('--ec_service_name', help="Name of the elasticube service service.", nargs='+',
                        default=DEFAULT_EC_SERVICE_NAME)
    # parser.add_argument('--ec_data_folder', help="The ElasricubeDataFolders", nargs='+',
    #                         default=ELASTICUBE_DEFAULT_DATA_FOLDER, type=is_dir)

    # parser.add_argument('-t', '--type', choices=['list_only', 'delete'], required=True)
    return parser.parse_args()


async def delete_folder(folder):
    if ACTION == 'delete':
        shutil.rmtree(folder)
        return " Folder \"" + folder + "\" deleted."
    else:
        return ""


async def clean_elasticube_data_folder(ec_name):
    ec_info = await get_active_elasticube_info(ec_name)
    logger.info("Scanning data folder for elasticube: " + ec_info['Title'])
    ec_folders = await get_ec_data_folders(ec_info['ID'], ec_info['DBFarmDirectory'])

    delete_tasks = []

    if ec_info['IsProcessing'] == 'False' and ec_info['IsRestarting'] == 'False' \
            and ec_info['IsLocked'] == 'False' and ec_info['IsStopping'] == 'False' and ec_info['IsInvalid'] == 'False':

        folders_list_str = "\t"

        for folder in ec_folders:


            if folder.lower() != ec_info['DBFarmDirectory'].lower():

                folders_list_str = folders_list_str + folder + " \n\t"
                delete_tasks.append(asyncio.create_task(delete_folder(folder)))

        if len(delete_tasks) > 0:

            folders_str = ""

            if len(delete_tasks) == 1:
                folders_str = "folder"
            else:
                folders_str = "folders"

            logger.info("\tFound " + str(len(delete_tasks)) + " " + folders_str + " to delete.")
            logger.info(folders_list_str)
            # logger.info('Start:' + time.strftime('%X'))

            for res in asyncio.as_completed(delete_tasks):
                compl = await res

                if compl != '':
                    logger.info(f'res: {compl} completed at {time.strftime("%X")}')

                    # logger.info('End:' + time.strftime('%X'))
        else:
            logger.info("\tNo folders to delete.\n")
        # print(f'Both tasks done: {all((t.done(), t2.done()))}')



    elif ec_info['IsProcessing']:

        logger.info(ec_info['Title'] + " is being built. Skipping folder cleanup.")

def is_path_in_list(path, path_list):

    for i in path_list:

        if i.lower() == path.lower():
            return True

    return False

def is_ec_service_running():

    service = (psutil.win_service_get(EC_SERVICE_NAME)).as_dict()

    return  True if service['status'] == 'running' else False
    
# -----------------------------------------Main-----------------------------------------------------------------------------
async def main():
    try:
        # Init logging
        # log_file_name = 'clean_' + LOG_DATE + '.log'
        init_logging(logger)
        logger.debug("Logger initialized")

        global ELASTICUBE_DATA_FOLDERS
        global ACTION
        global EC_SERVICE_NAME

        args = get_args()
        logger.debug("Args: " + str(args))

        ELASTICUBE_DATA_FOLDERS = args.ec_data_folders

        ACTION = args.action

        EC_SERVICE_NAME = args.ec_service_name

        if not is_ec_service_running():

            msg = EC_SERVICE_NAME + " is not running. Can't run data folder cleanup."
            # raise Exception(msg)
            print(msg)
            sys.exit(0)

        active_ec_list = await get_active_elasticubes_list()
        # print(active_ec_list)

        await asyncio.gather(*(clean_elasticube_data_folder(n) for n in active_ec_list))

    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    asyncio.run(main())
