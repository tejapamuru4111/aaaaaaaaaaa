import copy
import io
import traceback
import zipfile
import itertools
import boto3
import uuid
import docker
import tarfile
import subprocess
import shlex
from typing import Dict, Union

from apscheduler.schedulers.background import BackgroundScheduler
from botocore.client import Config
from botocore.exceptions import ClientError
from flatten_dict import flatten
from flatten_dict import unflatten
from flatten_dict.reducer import make_reducer
from flatten_dict.splitter import make_splitter
from file_read_backwards import FileReadBackwards
from concurrent.futures import ThreadPoolExecutor, wait
from flask import Flask, jsonify, make_response, \
    render_template, request, send_file, send_from_directory
from flask_socketio import SocketIO, emit
from json import loads

import ds.supported_ds
from db.db_utils import get_dq_rules_view_query
from db.userdata import Userdata
from ds.supported_ds import SUPPORTED_DS
from globals.global_utils import serialise_implementation_class, get_json_file_data

from scheduler import scheduler_worker
from alerts.alert_background_workers import alert_generator
from db import metadata
from mapping import mapper
from db.scheduler_metadata import *
from copy import deepcopy
from framework_utils.cache.redis_internal.redis_connection import init_redis_connection_pool
from ds.workload_data.query_template_schedule_util import get_query_template_to_schedule
from db.es_data import EsData

from ds.ds_utils import create_default_dq_ds
import base64
from queue_manager.queue_int import QueueManager
from alerts.alerts_constants import SINGLE_WORKER, MULTIPLE_WORKER



def get_ui_root_dir():
    """Get UI directories; these are different in the prod deployment and
    development. We differentiate based on the S2P_ROOT env variable.
    """
    ui_static = None
    ui_template = None

    if 'S2P_ROOT' in os.environ:
        ui_static = os.path.join(os.environ['S2P_ROOT'], "build", "static")
        ui_template = os.path.join(os.environ['S2P_ROOT'], "build")
    else:
        ui_static = os.path.join("..", "build", "static")
        ui_template = os.path.join("..", "build")

    return ui_static, ui_template


#
# ui_static, ui_template = get_ui_root_dir()
# app = Flask(__name__, static_folder=ui_static,
#             template_folder=ui_template)
# socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins="*")


def get_form_details(form):
    """
    :param form: request.form object from the http(s) requests.
    :return: dictionary with user and job details.
    """
    f_d = {}

    f_d['user_id'] = 'DD10' if 'user_id' not in form else form['user_id']
    f_d['job_name'] = 'DIOS_JOB' if 'job_name' not in form else form['job_name']
    f_d['job_type'] = 'S2P' if 'job_type' not in form else form['job_type']
    f_d['job_desc'] = '' if 'job_desc' not in form else form['job_desc']
    f_d['input_file'] = '/sample/file/location.xml' if 'input_file' not in form else form['input_file']
    f_d['s3_file_name'] = None if 's3_file_name' not in form else form['s3_file_name']
    return f_d


def handle_user_not_found(user_id):
    """
    :param user_id:
    :return: None if user_id not found or failure json
    """
    with metadata.Metadata() as md:
        user_exist = md.user_id_exists(user_id)

    if not user_exist:
        log.error("USER ID NOT FOUND: " + user_id)
        return json.dumps({'STATUS': 'FAIL',
                           'MESSAGE': 'USER ID [{}] DOES NOT EXIST'.format(user_id)}), 200
    else:
        return None


def handle_exception(message):
    pass


def aggregate_summary(result, summary_list):
    """
    :param result:
    :param summary_list
    :return: aggregated dictionary of result and summary_list
    """
    try:
        for summary_dict in summary_list:
            summary_flat = flatten(summary_dict, reducer=make_reducer(delimiter='_'))
            result_flat = flatten(result, reducer=make_reducer(delimiter='_'))

            for k, v in summary_flat.items():
                if k not in result_flat.keys():
                    result_flat.setdefault(k, v)
                else:
                    if isinstance(v, int):
                        result_flat[k] += summary_flat[k]
                    elif isinstance(v, list):
                        result_flat[k].extend(summary_flat[k])

            result = unflatten(result_flat, splitter=make_splitter(delimiter='_'))
    except:
        return result

    return result


def stream_data(file_name, data_or_stream=None):
    """ Stream data line by line
    """
    # If input is bytes, create io from bytes
    if isinstance(data_or_stream, bytes):
        file_stream = io.BytesIO(data_or_stream)
    else:
        file_stream = data_or_stream

    if file_name.endswith(ZIP_FORMAT):
        with zipfile.ZipFile(file_stream) as zip_file:
            fd = zip_file.open(zip_file.infolist()[0])
            for line in fd:
                yield line
    else:
        for line in file_stream:
            yield line


def zip_to_io(zipper):
    """ Convert zipped data to bytes
    """
    zip_buffer = io.BytesIO()
    for data in zipper:
        zip_buffer.write(data)
    return zip_buffer


def get_converted_file_name(in_file_name):
    extn = '.zip' if in_file_name.endswith(ZIP_FORMAT) else '.xml'
    converted_filename = in_file_name.replace(extn, '')
    return '{}_parallel_converted{}'.format(converted_filename, extn)


def get_s3_file(s3_file_path):
    """
    Function for downloading the file from S3 bucket
    :param s3_file_path: full path of the file on S3 bucket. (user_id/input or output/file_name.ext)
    :return: file_data if download is successful, False otherwise
    """
    if s3_file_path == '':
        return False

    try:
        log.info('CONNECTING TO S3')
        s3 = boto3.client(
            's3',
            'us-east-2',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    except Exception as e:
        log.error('FAILED TO CONNECT TO S3: {}'.format(e))
        return False

    try:
        log.info('DOWNLOAD STARTED, WAITING FOR DOWNLOAD TO BE COMPLETED...')
        file_data = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_file_path)['Body'].read()
        log.info('DOWNLOAD COMPLETED')
        return file_data
    except Exception as e:
        log.error('FAILED TO DOWNLOAD THE FILE FROM S3: {}'.format(e))
        return False


def create_s3_folder(user_id):
    """
    creates folder for a user in S3
    :param user_id: user_id of the user
    :return: True if creating the folder is successful, false otherwise
    """
    try:
        s3 = boto3.client(
            's3',
            'us-east-2',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        user_folder_name = '{}/'.format(user_id)
        user_ip_folder_name = '{}input/'.format(user_folder_name)
        user_op_folder_name = '{}output/'.format(user_folder_name)
        s3.put_object(Bucket=S3_BUCKET_NAME, Body='', Key=user_folder_name)
        s3.put_object(Bucket=S3_BUCKET_NAME, Body='', Key=user_ip_folder_name)
        s3.put_object(Bucket=S3_BUCKET_NAME, Body='', Key=user_op_folder_name)

        return True

    except Exception as e:
        log.error('FAILED TO CREATE S3 FOLDER: {}'.format(e))
        return False


def delete_objects_in_folder(s3, bucket, prefix):
    objects_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
    for obj in objects_to_delete:
        s3.delete_object(Bucket=bucket, Key=obj['Key'])


def delete_s3_folder(user_id):
    try:
        s3 = boto3.client(
            's3',
            'us-east-2',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        user_folder_name = '{}/'.format(user_id)
        user_ip_folder_name = '{}input/'.format(user_folder_name)
        user_op_folder_name = '{}output/'.format(user_folder_name)

        with ThreadPoolExecutor() as executor:

            input_folder_deletion = executor.submit(delete_objects_in_folder, s3, S3_BUCKET_NAME, user_ip_folder_name)
            output_folder_deletion = executor.submit(delete_objects_in_folder, s3, S3_BUCKET_NAME, user_op_folder_name)

        wait([input_folder_deletion, output_folder_deletion])

        delete_objects_in_folder(s3=s3, bucket=S3_BUCKET_NAME, prefix=user_id)

        return True

    except Exception as e:
        print(f"Error deleting S3 folder for user {user_id}: {e}")
        return False


def upload_file_to_s3(user_id, data, job_name, endpoint, ip_filename):
    """
    uploads the file to S3 bucket.
    :param user_id: user_id of the user.
    :param data: data that needs to be uploaded.
    :param job_name: name of the user job
    :param endpoint: the endpoint that is trying to upload file to s3(summary or s2p).
    :param ip_filename: name of the input file that was obtained from the user. used for checking the
     extension of the file(.xml or .zip)
    :return: dict that contains s3 file path and file name of the file if upload is successful, False otherwise
    """

    ip_filename = ip_filename.split('/')[2]
    di = {}

    try:
        log.info('CONNECTING TO S3')
        s3 = boto3.client(
            's3',
            'us-east-2',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        if endpoint == S2P_EP:
            # Ignore this section for now.
            file_name = '{}_parallel_converted.xml'.format(job_name)
            zip_file_name = '{}_parallel_converted.zip'.format(job_name)

        if ip_filename.endswith(ZIP_FORMAT):
            data_buffer = zip_to_io(data).getvalue()
        else:
            data_buffer = data
            data.seek(0)

        op_filename = get_converted_file_name(ip_filename)
        s3_file_path = '{}/output/{}'.format(user_id, op_filename)
        log.info('UPLOADING THE FILE TO S3')
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_file_path, Body=data_buffer)
        log.info('UPLOADING FINISHED')

        di['s3_file_path'] = s3_file_path
        presigned_url = create_presigned_url(S3_BUCKET_NAME, s3_file_path, 3600)
        di['presigned_url'] = presigned_url
        return di

    except Exception as e:
        traceback.print_exc()
        log.error('FAILED TO UPLOAD TO S3: {}'.format(e))
        return False


def create_presigned_url(bucket_name, s3_file_path, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param s3_file_path: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3 = boto3.client(
        's3',
        'us-east-2',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

    try:
        response = s3.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket_name,
                                                     'Key': s3_file_path},
                                             ExpiresIn=expiration)
    except ClientError as e:
        logging.error('FAILED TO CREATE PRESINGED URL: {}'.format(e))
        return None

    # The response contains the presigned URL
    return response


def create_presigned_post_url(s3_file_path):
    """ Generates a presigned URL for uploading the file.
    :param s3_file_path: string -- s3 path where the file needs to be uploaded.
    :return: dictionary that contains necessary information required for uploading the file. If error, returns None.
    """
    try:
        s3 = boto3.client(
            's3',
            'us-east-2',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4')
        )

        response = s3.generate_presigned_post(
            Bucket=S3_BUCKET_NAME,
            Key=s3_file_path
        )

    except ClientError as e:
        logging.error('FAILED TO CREATE PRESINGED URL: {}'.format(e))
        return None

    return response


def get_log_file_list(log_file_dir_path):
    try:
        filelist = [f for f in os.listdir(log_file_dir_path) if os.path.isfile(os.path.join(log_file_dir_path, f))]

        filelist.sort()
        file_list = []
        for f in filelist:
            # if f.endswith(".log") or f.endswith(".json") or '.log' in f:
            file_list.append(f)

        ret_data = {'STATUS': 'SUCCESS', 'MESSAGE': '', "DATA": file_list}
        return ret_data

    except Exception as e:
        ret_data = {'STATUS': 'FAIL', 'MESSAGE': e, "DATA": ''}
        log.info("ERROR GETTING THE LOG FILE LIST: {}".format(e))
        return ret_data


def fix_json_data(file_data_list):
    ret_list = []
    for data in file_data_list:
        if not data:
            continue
        data = data.strip()
        x = json.loads(data)
        ret_list.append(x)

    return ret_list


def get_log_file_dir() -> str:
    """
    return the log file directory path
    :return:
    """
    if 'S2P_ROOT' in os.environ:
        log_file_dir_path = '/var/log'
    else:
        # log_file_dir_path = os.path.join("tests", "expected_data")
        # user the below file path for local testing
        log_file_dir_path = os.path.join("..", "log")
    return log_file_dir_path


def get_log_file_path(log_file_name: str) -> str:
    """
    get the full path to log file , given the log_file_name
    :param log_file_name: log file name
    :return:
    """
    log_file_dir_path = get_log_file_dir()
    full_file_path = os.path.join(log_file_dir_path, log_file_name)
    return full_file_path


def read_server_log(start_point, offset, reverse, file_list, file_name):
    """
    Helper function for reading different types of server logs
    :param start_point: start point(line number) of the log file that needs to be retrieved
    :param offset: Number of lines requested
    :param reverse: flag for indicating tail functionality. If True then the file will read from the end
    :param file_list: Flag to indicate whether the file list being requested or the file data
    :param file_name: Name of the log file whose content being requested
    :return: Returns a dictionary that has status, message and the data field
    """
    try:
        ret_data = {'STATUS': 'FAIL', 'MESSAGE': '', "DATA": ''}
        start_point = int(start_point)
        if offset:
            offset = int(offset)
        log_file_dir_path = get_log_file_dir()

        full_file_path = get_log_file_path(file_name)

        # check to see if the list of available log files is requested
        if file_list == "TRUE":
            return get_log_file_list(log_file_dir_path)

    except Exception as e:
        log.error("LOG VIEWER: Error in log viewer: {}".format(e))
        return {'STATUS': 'FAIL', 'MESSAGE': e, 'DATA': ''}

    try:
        # check to see if the tail functionality is requested
        if reverse == "TRUE":
            file_ptr = FileReadBackwards(full_file_path, encoding="utf-8")
        else:
            file_ptr = open(full_file_path, 'r')

        if offset:
            file_slice = itertools.islice(file_ptr, start_point, start_point + offset)
        else:
            file_slice = itertools.islice(file_ptr, start_point, None)
        file_data_list = list(file_slice)
        str_data = ''.join(file_data_list)

        if reverse == "TRUE":
            file_data_list = list(reversed(file_data_list))
            str_data = '\n'.join(file_data_list)

        if file_name.endswith(".json"):
            str_data = fix_json_data(file_data_list)

        ret_data = {'STATUS': 'SUCCESS', 'MESSAGE': '', "DATA": str_data}
        file_ptr.close()
        log.info('Log file read successful')
        return ret_data

    except Exception as e:
        # file_ptr.close()
        log.info("LOG VIEWER: Error in reading the server log: {}".format(e))
        return {'STATUS': 'FAIL', 'MESSAGE': e, 'DATA': ''}


def add_default_users(dd_pw_hash, sch_pw_hash):
    log.info("ADDING DATA DIOS USER")
    db_username = os.environ.get('db_username', None)
    db_password = os.environ.get('db_password', None)
    db_url = os.environ.get('db_url', None)
    # md = metadata.Metadata()
    with metadata.Metadata() as md:
        if not md.user_exists(DD_MAIL):
            md.add_user(DD_FNAME, DD_LNAME, DD_MAIL, dd_pw_hash, is_tenant_signup="False",
                        user_type=USER_ADMIN, is_private_cloud="false", db_url=db_url,
                        db_username=db_username, db_password=db_password)
            md.add_permission(SUPER_USER, DEFAULT_UI_PERMISSIONS)
            check_partition_schedule_job()
            create_default_dq_ds(SUPER_USER)

        log.info("ADDING SCHEDULER USER")

        if not md.user_exists(SCHED_MAIL):
            md.add_user(SCHED_FNAME, SCHED_LNAME, SCHED_MAIL, sch_pw_hash, is_tenant_signup="False",
                        user_type=USER_ADMIN, is_private_cloud="false", db_url=db_url,
                        db_username=db_username, db_password=db_password)
            md.add_permission(SCHED_USER_ID, DEFAULT_UI_PERMISSIONS)


def validate_positive_integer(value: Union[str, int], default: int, name: str) -> int:
    """
    Validate and convert a value to a positive integer.
    :param value:
    :param default:
    :param name:
    :return:
    """
    try:
        int_value = int(value)
    except (ValueError, TypeError):
        log.warning(f"Invalid {name} value, defaulting to {default}: {value}")
        return default
    if int_value < 0:
        log.warning(f"Invalid {name}, Provided negative number defaulting to {default}: {value}")

    return int_value if int_value >= 0 else default


def build_search_command_and_exec(search_string: str, file_path: str, reverse: bool, start_time: str = None, end_time: str = None ) -> list:
    """
    Build secure shell command for log searching.
    :param search_string:
    :param file_path:
    :param reverse:
    :param start_time:
    :param end_time:
    :return:
    """

    def _is_safe_input(value: str) -> bool:
        """
        Validate proper input string
        :param value:
        :return:
        """
        return bool(re.match(r"^[\w\s\-\.:]*$", value))

    def _validate_datetime_format(dt_str: str):
        """
        validate proper datetime string
        :param dt_str:
        :return:
        """
        try:
            datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        except ValueError:
            raise ValueError(f"Invalid datetime format: {dt_str}")

    if not _is_safe_input(search_string):
        raise ValueError("Unsafe input detected")

    # Sanitize all inputs
    safe_search = shlex.quote(search_string)
    safe_path = shlex.quote(file_path)

    if start_time or end_time:
        _validate_datetime_format(start_time)
        safe_start = shlex.quote(start_time) if start_time else ''
        end_time = end_time if end_time else datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        _validate_datetime_format(end_time)
        safe_end = shlex.quote(end_time)

        # form awk command to get the logs with filters
        cmd_list = ["/usr/bin/awk", "-v", f"search={safe_search}", "-v", f"start={safe_start}", "-v", f"end={safe_end}",
                    f"$0 ~ search && ($1 \" \" $2) >= start && ($1 \" \" $2) <= end", f"{safe_path}"]
    else:
        # form the awk command to get the logs by search
        cmd_list = ["/usr/bin/awk", "-v", f"search={safe_search}", "$0 ~ search", f"{safe_path}"]

    # Execute command
    cmd_res = subprocess.run(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if cmd_res.returncode == 0 and cmd_res.stdout.strip() and reverse and platform.system() == 'Linux':
        cmd_res = subprocess.run(["/usr/bin/tac"], input=cmd_res.stdout, capture_output=True, text=True, check=True)

    if cmd_res.stderr:
        raise RuntimeError(f"Command failed: {cmd_res.stderr}")

    lines = cmd_res.stdout.splitlines()

    return lines


def search_in_log(start_point: Union[str, int], offset: Union[str, int], reverse: str, file_list: str, file_name: str,
                  search_string: str, log_start_time: str, log_end_time: str) -> Dict[str, Union[str, Dict]]:
    """
    Helper function for searching a text in server log
    :param log_end_time: filter end timestamp
    :param log_start_time: filter start timestamp
    :param file_list: Flag to indicate whether the file list being requested or the file data
    :param reverse: flag for indicating tail functionality. If True then the file will read from the end
    :param start_point: start point(line number) of the log file that needs to be retrieved
    :param offset: Number of lines to be searched
    :param file_name: Name of the log file whose content needs to be searched
    :param search_string: string the that needs to be searched in the log file
    :return: Returns a dictionary that has status, message and the data field
    """
    timer_start = time.time()
    result = {STATUS: FAIL_MESSAGE, MESSAGE: EMPTY_MESSAGE, DATA: EMPTY_MESSAGE}
    try:
        # form the boolean values with input args
        is_reverse = reverse.upper() == "TRUE"
        is_file_list = file_list.upper() == "TRUE"

        # return the list of log files
        if is_file_list:
            return get_log_file_list(get_log_file_dir())

        # get the file full path
        full_path = get_log_file_path(file_name)

        # build the shell command and execute to get the logs
        lines = build_search_command_and_exec(search_string, full_path, is_reverse, log_start_time, log_end_time)

        # validate numeric parameters
        validated_start = validate_positive_integer(start_point, 0, "start_point")
        validated_offset = validate_positive_integer(offset, 20, "offset") if offset else None

        # adjust start_point for 1-based index
        validated_start = max(validated_start - 1, 0) if validated_start > 0 else 0

        # form pagination
        end_index = validated_start + validated_offset if validated_offset else None
        filtered_lines = lines[validated_start:end_index]

        # handle JSON files
        processed_data = fix_json_data(filtered_lines) if file_name.endswith(".json") else '\n'.join(filtered_lines)

        log.info(f"Log search completed in {time.time() - timer_start:.2f} seconds")

        result[STATUS] = SUCCESS_MESSAGE
        result[DATA] = processed_data
        result[MESSAGE] = "Log file read successfully"

        return result

    except Exception as e:
        log.error(f"Log search error: {str(e)}")
        result[MESSAGE] = f"Error in reading log file {e}"
        return result


def validate_scheduler_params(ip_args):
    """
    Helper function to check if all the required inputs are provided for the scheduler
    :param ip_args: input dictionary received from the client
    :return: Dictionary that contains the result of verification
    """
    result = {'STATUS': FAIL_MESSAGE, 'DATA': {}, 'MESSAGE': ""}
    required_scheduler_params = ['user_id', 'workflow_type', 'workflow_name',
                                 'interval', 'start_time']
    for param in required_scheduler_params:
        if param not in ip_args:
            msg = "({}) parameter not provided".format(param)
            result['MESSAGE'] = msg
            return result

    else:
        result['STATUS'] = SUCCESS_MESSAGE
        return result


def start_background_scheduler(worker, max_instances: int, interval: int = 60):
    """
    Function for starting the python background scheduler
    :param worker: Worker/function that needs to be triggered from the scheduler.
    :param max_instances: integer, maximum instances that can be running at time.
    :param interval: integer, scheduler trigger interval
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=worker, trigger="interval", seconds=interval,
                      max_instances=max_instances)
    log.debug("Number of maximum instances for scheduler: {}".format(max_instances))
    scheduler.start()


def start_scheduler():
    """
    Function for scheduling the scheduler worker
    """
    try:
        log.info(f"Gunicorn type: {GUNICORN_TYPE}")
        # start the background scheduler for datadios scheduler functionality
        if GUNICORN_TYPE in [MULTIPLE_WORKER, None]:
            log.info("starting the background scheduler")
            start_background_scheduler(scheduler_worker.run_scheduler, 60)
            executor = ThreadPoolExecutor()
            executor.submit(scheduler_worker.run_scheduler, True)

        # start another instance of background scheduler for alerts
        if GUNICORN_TYPE in [SINGLE_WORKER, None]:
            log.info("starting the background scheduler for alerts")
            start_background_scheduler(alert_generator, 60)

        return True

    except Exception as e:
        log.error("Error in starting the scheduler: {}".format(e))
        return False


def check_scheduler_functionality(req_data):
    """
    Helper function to check if the request for scheduling is actually originated from the scheduler function
    and not from some other client. We do that by checking the mac id of the device.
    :param req_data: dictionary that has 'is_scheduler_workflow' and 'mac_id' params
    :return True if the verification is successful, False otherwise
    """

    mac_id = hex(uuid.getnode())
    if req_data.get('mac_id') == mac_id:
        log.info("MAC ID verification success for scheduler user")
        return True
    else:
        log.error("mac id of the server and scheduler are not matching, aborting scheduler triggered workflow")
        return False


# def pause_scheduler():
#     md = SchedulerMetadata()
#     if ENV == 'local':
#         return
#     md.pause_running_wf()
#     md.close_session()


def check_partition_schedule_job() -> None:
    """
    check and update the scheduler jobs
    :return: None
    """
    with SchedulerMetadata() as sdb:
        schedule_job = sdb.schedule_partition_maintenance_job()
        if schedule_job['STATUS'] != SUCCESS_MESSAGE:
            log.error("Fail to schedule partition maintenance job {}".format(schedule_job['MESSAGE']))
        else:
            log.info(schedule_job.get('MESSAGE'))


def get_all_users_wl_registered_ds_list(users: list) -> list:
    """
    function for get the all users and ds_names registered for workload
    :param users:
    :return:
    """
    users_wl_registered_ds_list = []
    for user in users:
        with Userdata(user['user_id']) as udb:
            ds_list = udb.get_all_wl_registered_ds()
            users_wl_registered_ds_list += ds_list

    return users_wl_registered_ds_list


def check_default_dq_hub(users: list) -> bool:
    """
    Create DataQuality Hub for existing users
    :param users:
    :return:
    """
    view_query = get_dq_rules_view_query()
    for user in users:
        user_id = user.get('user_id')
        if not user_id:
            continue
        sts = create_default_dq_ds(user_id)
        if not sts:
            log.debug(f"Failed to create DataQuality Hub for user {user_id}")
        else:
            log.debug(f"DataQuality Hub is created/existed for user {user_id}")

        # create views for already existing users
        with Userdata(user_id) as udb:
            try:
                udb.session.execute(text(view_query.format(user_id)))
                udb.session.commit()
            except Exception as e:
                log.error(f"Error in creating Data Quality view for user: {user_id} \n{e}")
                udb.session.rollback()

    return True


def schedule_wl_query_template_for_every_user(users_wl_registered_ds_list: list):
    """
    function for scheduling the workflows of already existing users
    :param users_wl_registered_ds_list:
    :return:
    """
    if users_wl_registered_ds_list:
        for user_ds in users_wl_registered_ds_list:
            user_id = user_ds["user_id"]
            ds_name = user_ds["ds_name"]
            query_template_to_schedule = get_query_template_to_schedule(ds_name, user_id)
            with SchedulerMetadata() as smd:
                smd.schedule_query_template(query_template_to_schedule)


def startup_upsert():
    try:
        log.info("INITIALIZING THE {} DB".format(DB_TYPE))
        with metadata.Metadata() as md:
            log.info("INITIALIZING THE METADATA")
            # initialize redis connection pool
            init_redis_connection_pool()

            # md.init_mdb() # commented as we are doing this operation before startup_upsert in flask_server.py
            md.upsert_mapping_jsons(mapper.get_stages())

            # upsert supported ds json
            supported_ds_copy = copy.deepcopy(
                ds.supported_ds.SUPPORTED_DS)  # deep copy to avoid source data modification

            # serialisation of 'Implementation Class' field
            serialised_supported_ds = serialise_implementation_class(supported_ds_copy)

            # store the supported json in the db
            md.supported_ds_handle(operation=UPSERT, json_data=serialised_supported_ds)

            supported_db_names = SUPPORTED_DS.keys()
            md.update_wl_metric_jsons(get_wl_metric_json_file_data(), startup=True)
            md.update_json_def(get_json_file_data(file_type=EXECUTION_RESULT, supported_ds_data=supported_ds_copy),
                               json_def_type=EXECUTION_RESULT, startup=True)

            md.update_wl_query_template_jsons(get_query_template_json_file_data(), startup=True)
            # md.wl_lookup_map_creation(supported_db_names, startup=True)
            # schedule_wl_query_template_for_every_user(get_all_users_wl_registered_ds_list(users))

            # checking the default partition job exist or not
            check_partition_schedule_job()
            # TODO: Maintain app update version for user (like metadata version) to skip user specific updates -> Teja

            return True

    except Exception as e:
        log.error("Error in startup upsert: {}".format(e))
        return False


def get_wl_metric_json_file_data():
    supported_db_names = SUPPORTED_DS.keys()
    res = {}
    for ds_type in supported_db_names:
        metric_json_file_path = os.path.join(WL_METRIC_DEF_DIR, "{}.json".format(ds_type))
        if os.path.exists(metric_json_file_path):
            try:
                with open(metric_json_file_path, "r", encoding='utf-8') as my_file:
                    met_def_json_data = loads(my_file.read())
                res[ds_type] = met_def_json_data
            except Exception as e:
                log.error(
                    "Error while loading the workload metric json , ds_type: '{0}' , file_path : '{1}'\nErrors : {2}".format(
                        ds_type, metric_json_file_path, e))
    return res


def get_query_template_json_file_data():
    from json import loads
    supported_db_names = deepcopy(SUPPORTED_DS)
    supported_db_names.update({GLOBAL_DASHBOARD: {}})
    supported_db_names = supported_db_names.keys()
    res = {}
    for ds_type in supported_db_names:
        query_template_json_file_path = os.path.join(WL_QUERY_TEMPLATE_DEF_DIR, "{}.json".format(ds_type))
        if os.path.exists(query_template_json_file_path):
            try:
                with open(query_template_json_file_path, "r", encoding='utf-8') as my_file:
                    query_template_def_json_data = loads(my_file.read())
                res[ds_type] = query_template_def_json_data
            except Exception as e:
                log.error(
                    "Error while loading the workload query template json , ds_type: '{0}' , file_path : '{1}'\nErrors : {2}".format(
                        ds_type, query_template_json_file_path, e))
    return res


def encrypt(text):
    """
    :param text: sample text
    :return: encrypted form of text
    """
    encoded_bytes = text.encode('utf-8')
    encrypted_bytes = base64.b64encode(encoded_bytes)
    return encrypted_bytes.decode('utf-8')


def decrypt(encrypted_text):
    """

    :param encrypted_text: Given encrypted text
    :return:  original text
    """
    encrypted_bytes = base64.b64decode(encrypted_text.encode('utf-8'))
    return encrypted_bytes.decode('utf-8')


def create_sample_s3_folder(user_id):
    if 'SERV_ENV' in os.environ:
        if not os.environ['SERV_ENV'] == 'docker_local':
            if create_s3_folder(user_id):
                log.info('USER S3 FOLDER CREATED')
            else:
                return {'STATUS': 'FAIL', 'MESSAGE': 'ERROR CREATING S3 FOLDER'}

    else:
        if create_s3_folder(user_id):
            log.info('USER S3 FOLDER CREATED')
        else:
            return {'STATUS': 'FAIL', 'MESSAGE': 'ERROR CREATING S3 FOLDER'}
    return {'STATUS': 'SUCCESS', 'MESSAGE': 'USER S3 FOLDER CREATED SUCCESSFULLY'}


def get_running_container_names():
    """
    Helper function to get the names of all the running docker containers
    : return: List that contains names of the containers
    """
    client = docker.from_env()
    containers = client.containers.list()
    container_list = [container.name for container in list(containers)]
    return container_list


def get_exited_container_names():
    client = docker.from_env()
    containers = client.containers.list(all=True)
    stopped_containers = [container.name for container in containers if container.status == 'exited']
    return stopped_containers


def download_s3_file(bucket_name, file_path, target_path):
    """
    Download AWS S3 file
    @param bucket_name: Bucket name
    @param file_path: file path in S3
    @param target_path: Relative target path
    @return: Boolean indicates whether file is downloaded or not
    """
    target_path = os.path.abspath(target_path)
    log.info('CONNECTING TO S3')
    s3 = boto3.client(
        's3',
        'us-east-2',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    try:
        s3.download_file(bucket_name, file_path, target_path)
        return True
    except Exception as e:
        log.error("Error in downloading s3 file: {}".format(e))
        return False


def download_es_cert():
    """
    Function to download the elastic search certificate from S3
    @return: Boolean indicates whether the certificate is downloaded or not
    """
    log.info("trying to download cert file from s3")
    return download_s3_file(ELASTIC_BUCKET_NAME, ELASTIC_CERT_FILE, ELASTIC_CERT_PATH)


def is_port_in_use(port: int) -> bool:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def copy_local_es_certificate(container_name):
    """
    Function to copy the certificate from local elasticsearch container.
    @param container_name: Elasticsearch container name
    @return: Boolean indicates whether the certificate is copied successfully or not
    """
    try:
        docker_client = docker.from_env()
        container = docker_client.containers.get(container_name)
        time.sleep(20)
        bits, stat = container.get_archive(ELASTIC_CERT_FILE_PATH)
        tar_stream = io.BytesIO()
        for chunk in bits:
            tar_stream.write(chunk)
        tar_stream.seek(0)

        with tarfile.open(fileobj=tar_stream) as tar:
            member = tar.getmembers()[0]
            file_content = tar.extractfile(member).read()

            with open(ELASTIC_CERT_PATH, 'wb') as f:
                f.write(file_content)
        return True
    except Exception as e:
        log.error(f"Error in starting the Elasticsearch and RabbitMQ Docker container: {e}")
        return False


def check_local_es_status():
    """
    Check elasticsearch status
    :return: Boolean indicates whether elasticsearch is running or not
    """
    time_count = 0
    while True:
        es = EsData()
        if es.es.ping():
            break
        time.sleep(4)
        time_count += 1
        if time_count > 60:
            return False
    return True


def start_es():
    """
       Function for starting the Elasticsearch Docker container in the local dev environment
       :return: True if the Docker container starts successfully, False otherwise
    """
    try:
        log.info("starting es")
        es_container = "es_local"
        running_container_list = set(get_running_container_names())

        if es_container in running_container_list:
            log.info("Elasticsearch container is already running")
            res = copy_local_es_certificate(es_container)
            if not res:
                return res
            return check_local_es_status()

        # Create elastic container volume path and give write permission
        # Elasticsearch will run by non-root user.
        # so volume dir needs the write permission for non-root users also.
        if not os.path.exists(os.getenv("ELASTIC_VOL_PATH")):
            os.makedirs(os.getenv("ELASTIC_VOL_PATH"))
            os.chmod(os.getenv("ELASTIC_VOL_PATH"), 0o740)

        docker_client = docker.from_env()

        stopped_containers = get_exited_container_names()

        if es_container in stopped_containers:
            log.info("es_local container is stopped, starting it now")
            container = docker_client.containers.get(es_container)
            container.start()
            log.info(f"Started container: {es_container}")

        running_container_list = set(get_running_container_names())

        if es_container not in running_container_list:
            subprocess.Popen(["docker-compose", "-f", "es-docker-compose.yml", "up", "-d"])
            log.info("Docker Compose services started successfully.")

        time_count = 0
        while True:
            # Check the elastic local is started and the elastic setup is stopped.
            # Once the certificate creation is done, the elastic setup will exit.
            if ("es_local" in get_running_container_names() and
                    "es_setup" not in get_running_container_names()):
                break
            time.sleep(4)
            time_count += 1
            if time_count > 60:
                log.error("Time out waiting for Elasticsearch container to start")
                return False
        time.sleep(10)
        res = copy_local_es_certificate("es_local")
        if not res:
            return res
        return check_local_es_status()
    except Exception as e:
        log.error(f"Error in starting the Elasticsearch and RabbitMQ Docker container: {e}")
        return False


def start_rabbitmq():
    try:
        log.info("starting rmq")

        rm_container = "rabbitmq_local"
        running_container_list = set(get_running_container_names())

        if rm_container in running_container_list:
            log.info("RabbitMq container is already running")
            return True

        docker_client = docker.from_env()

        stopped_containers = get_exited_container_names()

        if rm_container in stopped_containers:
            log.info("rabbitmq_local container is stopped, starting it now")
            container = docker_client.containers.get(rm_container)
            container.start()
            log.info(f"Started container: {rm_container}")

        running_container_list = set(get_running_container_names())

        if rm_container not in running_container_list:
            subprocess.Popen(["docker-compose", "-f", "rabbitmq-docker-compose.yml", "up", "-d"])
            log.info("RabbitMQ Docker Compose service started successfully.")

        time_count = 0
        while "rabbitmq_local" not in get_running_container_names():
            time.sleep(1)
            time_count += 1
            if time_count > 60:
                log.error("Time out waiting for RabbitMQ container to start")
                return False
        time.sleep(10)
        log.info("Started the RabbitMQ Docker container successfully")

        return True

    except Exception as e:
        log.error(f"Error in starting the RabbitMQ Docker container: {e}")
        return False


def start_local_pg_db():
    """
    Function for starting the postgres docker container in the local dev env
    :return: True if the docker container starts successfully, False otherwise
    """

    try:
        container_list = get_running_container_names()
        if "pg_local" in container_list:
            log.info("container already running")
            return True

        docker_client = docker.from_env()
        images = docker_client.images.list()
        for image in list(images):
            if "pg_docker" in str(image):
                break

        else:
            log.info("Building the postgres docker image...")
            docker_client.images.build(tag='pg_docker', dockerfile="dockerfile_pg",
                                       path=os.path.abspath(os.path.join(os.getcwd(), os.pardir)))
            log.info("Building the postgres docker images finished successfully")

        pwd = os.getcwd()
        volume_path = os.path.join(pwd, 'pg_data')
        docker_client.containers.run(image='pg_docker:latest', ports={'5432/tcp': 5432}, name='pg_local', detach=True,
                                     volumes={
                                         volume_path: {'bind': '/var/lib/postgresql/data',
                                                       'mode': 'rw'}}, auto_remove=True)

        log.info("Starting the postgres docker container...")
        running_containers = get_running_container_names()
        time_count = 0
        while "pg_local" not in running_containers:
            time.sleep(1)
            time_count += 1
            if time_count > 60:
                break
            running_containers = get_running_container_names()
        try_cnt = 5
        while try_cnt > 0:
            try:
                conn = DB({"user_id": 'metadata'})
                break
            except Exception as e:
                try_cnt -= 1
                time.sleep(5)

        log.info("Started the postgres docker container successfully")
        return True
    except Exception as e:
        log.error("Error in starting the postgres docker container: {}".format(e))
        return False


def handle_sigterm(*args):
    """
    SIGTERM and SIGINT signal handler function
    The flask server enters this function before exiting. Any required handling can be written here.
    """
    log.info("Inside sigterm handler")
    docker_client = docker.from_env()

    try:
        log.info("stopping the docker containers")
        # containers = docker_client.containers.list()
        # containers_to_stop = ['pg_local', 'es_local', 'rabbitmq_local']
        # stopped_containers = []
        # for container in containers:
        #     if container.name in ['pg_local', 'es_local', 'rabbitmq_local']:
        #         log.info("stopping the {} docker container".format(container.name))
        #         container.stop()
        #         log.info("Stopped the {} docker container".format(container.name))
        #         stopped_containers.append(container.name)
        # stopped_containers = set(stopped_containers)
        # for container_name in containers_to_stop:
        #     if container_name not in stopped_containers:
        #         log.info("{} docker container stopped already".format(container_name))

    except Exception as e:
        log.error(f'Failed to stop the containers: {e}')


def publish_load_features(user_detail):
    # from db.es_utils import load_uss_features
    try:

        request_args = {
            'user_id': user_detail['user_id'],
            'tenant_id': user_detail['tenant_id'],
            'features': user_detail['features'],
            'ds_name': user_detail.get('ds_name'),
            'search_type': user_detail.get('search_type'),
            'ai_ds_type': user_detail.get('ai_ds_type')
        }
        queue_manager = QueueManager("rabbitmq", 'celery')
        payload_dict = {
            'task': 'load_uss_features',
            'request_args': request_args}
        queue_manager.publish_task(**payload_dict)
        # load_uss_features(**payload_dict)
        return {'STATUS': 'SUCCESS', 'MESSAGE': f'Task submitted for {user_detail["user_id"]}'}
    except Exception as e:
        return {'STATUS': 'FAIL', 'MESSAGE': f'Failed to submit task for {user_detail["user_id"]}: {str(e)}'}


def prepare_swagger_json():
    """
    Function for combining all the JSONs to form a single swagger JSON
    """
    data = {}
    master = {}
    try:
        for root, dirs, files in os.walk(SWAGGER_STATIC_DIR):
            for file_name in files:
                if not file_name.endswith(JSON_FORMAT) or file_name == SWAGGER_FILE:
                    continue
                with open(os.path.join(root, file_name), 'r') as f:
                    if file_name == MASTER_SWAGGER_JSON_FILE:
                        master.update(json.load(f))
                    else:
                        json_data = json.load(f)
                        data.update(json_data['path'])

        master['paths'].update(data)
        swagger_file_path = os.path.join(SWAGGER_STATIC_DIR, SWAGGER_FILE)
        with open(swagger_file_path, "w") as f:
            json.dump(master, f, indent=4)

    except Exception as e:
        log.error("Error in Swagger json file preparation, Swagger UI may not work as expected: {}".format(e))


def build_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response



