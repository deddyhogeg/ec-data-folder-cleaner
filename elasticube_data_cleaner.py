# -*- coding: utf-8 -*-
'''
Python 3.5.1
Author : Dan Kushner <dan.kushner@sisense.com>
This Script was Written to clean ElastiCubeData folder

Version 3.0
-----------
Author : David Hogeg <david.hogeg@sisense.com>
'''
import time
import datetime
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

logger = logging.getLogger(__name__)

# Get settings
settings = yaml.load(open(application_path + "\settings.yaml", 'r'))

# Init global vars
START = time.time()
APP_DIR = "C:\ProgramData\Sisense\PrismServer\ElastiCubeData_Cleaner"
ELASTICUBE_DEFAULT_DATA_FOLDER = "C:\ProgramData\Sisense\PrismServer\ElastiCubeData"
PSM_DEFAULT_PATH = "C:\\Program Files\\Sisense\\Prism\\Psm.exe"
LOG_DATE = str(datetime.date.today())

# Create new APP_DIR folder for the logs if doesn't exist
if not os.path.exists(APP_DIR):
    os.makedirs(APP_DIR)
    os.makedirs(APP_DIR + '\logs')

if not os.path.exists(settings['BACKUP_FOLDER_PATH'] + '\ElastiCubeData_backup'):
    os.makedirs(settings['BACKUP_FOLDER_PATH'] + '\ElastiCubeData_backup')

if not os.path.exists(settings['BACKUP_FOLDER_PATH'] + '\ElastiCubeData_sql_logs_backup'):
    os.makedirs(settings['BACKUP_FOLDER_PATH'] + '\ElastiCubeData_sql_logs_backup')

# TODO clean up this section. No need for the ROOT var.
ROOT = None

if ROOT is None:
    if settings['ELASTICUBE_DATA_FOLDER'] is not None:
        ROOT = settings['ELASTICUBE_DATA_FOLDER']
        logger.debug("Using Elasticube data folder from settings file: {0}".format(settings['ELASTICUBE_DATA_FOLDER']))
    else:
        logger.debug("Using default Elasticube data folder: {0}".format(ELASTICUBE_DEFAULT_DATA_FOLDER))
        ROOT = ELASTICUBE_DEFAULT_DATA_FOLDER

if not os.path.exists(ROOT):
    logger.error('-F- Specified data folder does not exist: ' + str(ROOT))
    sys.exit(0)


# ------------------------------------------------------------------------------------------

def delete_logs_files(directory, zip_or_log):
    try:
        for file in os.listdir(directory):
            if file.endswith('.log') or file.endswith('.zip'):
                fullpath = os.path.join(directory, file)  # turns 'file1.txt' into '/path/to/file1.txt'
                timestamp = os.stat(fullpath).st_ctime  # get timestamp of file
                createtime = datetime.datetime.fromtimestamp(timestamp)
                now = datetime.datetime.now()
                delta = now - createtime
                if delta.days > 30 and zip_or_log == 'log':
                    try:
                        os.remove(fullpath)
                        logger.info("-S- Deleted log: " + str(fullpath))
                    except Exception as e:
                        logger.debug('-F- Couldnt delete log ' + str(fullpath))
                        logger.debug(e)
                if delta.days > 30 and zip_or_log == 'zip':
                    try:
                        os.remove(fullpath)
                        logger.info("-S- Deleted log: " + str(fullpath))
                    except Exception as e:
                        logger.debug('-F- Couldnt delete zip ' + str(fullpath))
                        logger
            else:
                logger.info("-S- No logs to delete")
                return
    except Exception as e:
        logger.debug('-F- Couldnt list files in: ' + str(directory))
        logger.debug(e)


logger.info("-S- Starting delete old logs (more then 30 days)")
delete_logs_files(APP_DIR + '\logs', 'log')
delete_logs_files(APP_DIR + '\ElastiCubeData_beckup', 'zip')
delete_logs_files(APP_DIR + '\ElastiCubeData_sql_logs_beckup', 'zip')
logger.info("-S- Finished delete old logs (more then 30 days)")


# ---------------------------------------------------------------------------------------------------------------------------
def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)


def get_all_elasticube_folders(elasticube_data_folder):
    all_folders = list()
    for db_cube_folder in os.listdir(elasticube_data_folder):
        folder = os.path.join(elasticube_data_folder, db_cube_folder)
        all_folders.append(folder)
    return all_folders


def get_all_folders_to_erase(all, save):
    save_base_name = [os.path.basename(x) for x in save]

    logger.debug("Elasticubes to keep:\n{0}".format(save_base_name))

    remove_ElastiCube_files = list()
    for f in all:

        if os.path.basename(f) not in save_base_name:
            remove_ElastiCube_files.append(f)
            logger.debug("{0} not in elasticube save list and will be deleted.".format(os.path.basename(f)))

    logger.debug("{0} folders where added to the delete list.".format(len(remove_ElastiCube_files)))
    return remove_ElastiCube_files


def length_of_EC_files(folders):
    return len(folders)


def zip_dir(folder, zip_file_name):
    zfile = zipfile.ZipFile(os.path.join(folder, zip_file_name), 'x', zipfile.ZIP_DEFLATED)

    logger.info("Zipping data folder {0}...".format(zip_file_name))

    root_len = len(folder) + 1
    for base, dirs, files in os.walk(folder):
        for file in files:
            fn = os.path.join(base, file)
            zfile.write(fn, fn[root_len:])
    zfile.close()

    logger.info("\x1b[1A" + "Completed")


def save_to_archive(folders, logs_or_dbfarms):
    if logs_or_dbfarms == 'dbfarms':
        for folder in folders:
            # concatenate the folders for file name
            zipfilename = settings['BACKUP_FOLDER_PATH'] + '\ElastiCubeData_backup' + '\\' + os.path.basename(
                folder) + '_' + str(
                LOG_DATE) + '.zip'  # "%s.zip" % (folder.replace("/", "_"))
            try:
                zip_dir(folder, zipfilename)
            except Exception as e:
                logger.error(e)
                logger.debug(traceback.print_exc())

                # elif logs_or_dbfarms == 'sql_logs':
                #     for folder in folders:
                #         f = os.path.join(folder, 'sql_logs')
                #         zipfilename = DIRECTORY + '/ElastiCubeData_sql_logs_backup' + '/' + os.path.basename(folder) + '_' + str(
                #             LOG_DATE) + '.zip'
                #         try:
                #             zipdir(f, zipfilename)
                #         except:
                #             logger.debug('-F- Could not zip this folder ' + str(
                #                 f) + ' please check if this cube running/building/invalid consider stop the elasticube console server')
                #             logger.debug('-F- Tried to zip this folder ' + str(f))
                #             print('Couldnt zip dbframe please look at the logs')
                #             print('Fail to zip this folder ' + str(f))
                #             try:
                #                 input(
                #                     "Press enter to continue without archive the entire sql_logs of the cube that should be erased from the default data folder?")
                #             except SyntaxError:
                #                 pass


def save_files_statistic(all, save, delete):
    logger.info("------------------------------------------------------------------------")
    logger.info("Total number of Elasticube folders: " + str(length_of_EC_files(all)))
    logger.info("Total number of Elasticube folders to save:" + str(length_of_EC_files(save)))
    logger.info("Total nubmer of Elasticube folders to delete:" + str(length_of_EC_files(delete)))
    logger.info("------------------------------------------------------------------------")


def human(size):
    B = "B"
    KB = "KB"
    MB = "MB"
    GB = "GB"
    TB = "TB"
    UNITS = [B, KB, MB, GB, TB]
    HUMANFMT = "%f %s"
    HUMANRADIX = 1024.
    for u in UNITS[:-1]:
        if size < HUMANRADIX: return HUMANFMT % (size, u)
        size /= HUMANRADIX
    return HUMANFMT % (size, UNITS[-1])


def folder_size(folder):
    folder_size = 0
    for (path, dirs, files) in os.walk(folder):
        for file in files:
            filename = os.path.join(path, file)
            folder_size += os.path.getsize(filename)
    return folder_size


def delete_folder(delete):
    total_size = 0
    size = 0
    for folder in delete:
        try:
            size = folder_size(folder)
            total_size += size  # os.path.getsize(f)
            shutil.rmtree(folder)  # delete folder........!!os.remove(folder) or
            logger.info("Successfully deleted " + str(folder))
            # logger.info("-S- Deleted: " + folder)
            logger.info("Size: " + str(human(size)))  # human(os.path.getsize(f)))
        except Exception as e:
            logger.debug('-F- Could not delete folder: ' + str(folder))
            logger.debug(e)
    logger.info("-S- Total Folder Deleted Size: " + str(human(total_size)))


def get_active_elasticubes():
    # Set the env variable required to run the psm command
    os.environ['SISENSE_PSM'] = 'True'

    # PSM output structure: Cube Name [NW_A] ID : [aNWXwAaA] FarmPath [C:\ProgramData\SiSense\PrismServer\ElastiCubeData\aNWXwAaA_2016-11-15_23-14-59] Status [STOPPED]
    p_output = subprocess.check_output(
        [settings['PSM_PATH'], 'ecs', 'ListCubes', 'serverAddress=' + settings['SERVER_ADDRESS']])

    split_output = p_output.splitlines()

    # regex to extract the the cube info from the psm command output
    re_comp = re.compile(
        "^Cube Name \[(?P<name>.*)\] ID *.\s\[(?P<cube_id>.*)\] FarmPath \[(?P<dbfarm_path>.*)\] Status \[(?P<cube_status>.*)\]$")

    ec_dict = dict()

    for output_str in split_output:

        # output_str = ((str(output_str)).encode('utf-8', 'ignore'))

        m = re_comp.search(str(output_str, 'utf-8', 'ignore'))

        if m is not None:
            ec_dict[m.group('name')] = [m.group('cube_id'), m.group('dbfarm_path')]

    logger.debug(ec_dict)
    return ec_dict


def get_active_elasticubes_folder_list(active_ec_dict=None):
    if active_ec_dict is None:
        active_ec_dict = get_active_elasticubes().values()

    return [i[1] for i in active_ec_dict.values()]


def init_logging(logger, logFilePath, logFileName, fileLogLevel, consoleLogLevel):
    logger = logging.getLogger(__name__)
    logger.setLevel(fileLogLevel)

    # Create file handler

    fh = logging.FileHandler(logFilePath + logFileName)
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


def validate_settings():
    if settings['ELASTICUBE_DATA_FOLDER'] is None:
        # logger.debug("ELASTICUBE_DATA_FOLDER was not found in settings file. Using default: {0}".format(
        #     ELASTICUBE_DEFAULT_DATA_FOLDER))
        settings['ELASTICUBE_DATA_FOLDER'] = ELASTICUBE_DEFAULT_DATA_FOLDER

    if settings['BACKUP_FOLDER_PATH'] is None:

        settings['BACKUP_FOLDER_PATH'] = APP_DIR

    if settings['PSM_PATH'] is None:

        settings['PSM_PATH'] = PSM_DEFAULT_PATH

    if settings['FILE_LOG_LEVEL'] is None:

        settings['FILE_LOG_LEVEL'] = 'DEBUG'

    if settings['CONSOLE_LOG_LEVEL'] is None:

        settings['CONSOLE_LOG_LEVEL'] = 'DEBUG'

    if settings['SERVER_ADDRESS'] is None:

        settings['SERVER_ADDRESS'] = 'localhost'

    if settings['BACKUP_ELASTICUBE_BEFORE_DELETE'] is None:

        settings['BACKUP_ELASTICUBE_BEFORE_DELETE'] = False


# -----------------------------------------Main-----------------------------------------------------------------------------
def main():

    validate_settings()

    # Init logging
    log_file_name = 'clean_' + LOG_DATE + '.log'
    init_logging(logger, settings['LOG_FILE_PATH'], log_file_name, settings[
        'FILE_LOG_LEVEL'], settings['CONSOLE_LOG_LEVEL'])

    logger.debug("Logger initialized")

    parser = argparse.ArgumentParser(description='Clean up elasticube data folder.')
    parser.add_argument('-t', '--type', choices=['list_only', 'delete'], required=True)
    args = parser.parse_args()

    print(args.type)

    active_ec_dict = get_active_elasticubes()

    save_ec_folders_list = get_active_elasticubes_folder_list(active_ec_dict)

    full_ec_folders_list = get_all_elasticube_folders(settings['ELASTICUBE_DATA_FOLDER'])

    delete_ec_folders_list = get_all_folders_to_erase(full_ec_folders_list, save_ec_folders_list)

    save_files_statistic(full_ec_folders_list, save_ec_folders_list, delete_ec_folders_list)

    if len(delete_ec_folders_list) == 0:
        logger.info("-S- No ElastiCube folders to delete.")
    else:
        # Save to archive save_to_archive(folders, logs_or_dbfarms, ELASTICUBE_ARCHIVE):
        if settings['BACKUP_ELASTICUBE_BEFORE_DELETE'] is True:
            save_to_archive(delete_ec_folders_list, 'dbfarms')

        # print('Start Archive Sql_logs')
        # save_to_archive(delete_ElastiCube_file, 'sql_logs')
        if args.type == "list_only":
            logger.info("List of folders to delete (folders will NOT be deleted)")
            logger.info(delete_ec_folders_list)
        else:
            logger.info('Deleting folders:')
            delete_folder(delete_ec_folders_list)

    logger.info("--------------------------------------")
    logger.info("Informative log created here -> C:\ProgramData\Sisense\PrismServer\ElastiCubeData_Cleaner\logs")
    logger.info("Cleaning completed successfully :)")

    # try:
    #     input("Press enter to exit")
    # except SyntaxError:
    #     pass

    logger.info('It took: ' + str(time.time() - START) + ' seconds.')
    logger.info('Successful Finished')
    logger.info(
        '------------------------------------------------------------------------------------------------------')


if __name__ == '__main__':
    main()
