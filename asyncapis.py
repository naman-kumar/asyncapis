import os
import time
import re
# import httpx
import json
import getpass
import dataclasses
# from linkedin.http import tls
import urllib.parse
from pathlib import Path
from dfsrereinstate.utils import logger
from dfsrereinstate.utils import load_config
from dfsrereinstate.utils.constant import AZUrls, AzProjectName, ClusterName
from dfsrereinstate.utils.asyncapis import AsyncCall


class FailedLogin(ValueError):
    pass


class AzkabanError(Exception):
    pass


@dataclasses.dataclass
class Azkaban:
    az_url: str
    configs = load_config()
    log = logger.getLoggerInstance(__name__, configs.LOG_LEVEL)

    def __post_init__(self):
        home = str(Path.home())
        self.session_file_name = os.path.join(home, '.az_session.json')
        self.async_client = AsyncCall()

    def is_session_id_valid(self, session: dict):
        created_time_epoch = session.get('created')
        if created_time_epoch:
            lapse_time = time.time() - created_time_epoch
            if lapse_time < 60*60*11:  # secong * min * hour
                return True
        return False

    def get_session_id(self):
        if os.path.isfile(self.session_file_name):
            with open(self.session_file_name) as f:
                try:
                    sessions = json.load(f)
                except json.decoder.JSONDecodeError:
                    self.log.exception(f"Exception occurred while loading session file.")
                    return self.login()
                session = sessions.get(self.az_url)
                if session and self.is_session_id_valid(session) is True:
                    return session.get('session_id')
                else:
                    return self.login()
        else:
            return self.login()

    def write_session_file(self, data):
        with open(self.session_file_name, 'w') as f:
            f.write(json.dumps(data, indent=4))

    @staticmethod
    def get_user_name_password():
        user_name = input('Ldap User Name: ')
        password = getpass.getpass('Password + VIP: ')
        return user_name, password

    def login(self):
        counter = 0
        while counter < 3:
            username, password = self.get_user_name_password()
            data = {
                'action': 'login',
                'username': username,
                'password': password,
            }
            async_client = AsyncCall()
            async_client.push_post_http_param(url=self.az_url, data=data, retry=3)
            data = async_client.post()[0]
            # print(data)
            if data is None or data.get('error', '').startswith('Incorrect Login'):
                print('Authentication failed')
                counter += 1
            elif data.get('status') == 'success':
                session_created_time = time.time()
                if os.path.isfile(self.session_file_name):
                    with open(self.session_file_name, 'r') as f:
                        try:
                            sessions = json.load(f)
                            sessions[self.az_url] = {'created': session_created_time,
                                                     'session_id': data['session.id']}
                            self.write_session_file(sessions)
                            return data['session.id']
                        except json.decoder.JSONDecodeError:
                            os.remove(self.session_file_name)
                sessions = {self.az_url: {'created': session_created_time, 'session_id': data['session.id']}
                            }
                self.write_session_file(sessions)
                return data['session.id']
        print("Exceeded the maximum number of retries")

    def get_latest_flow_execution_id(self, az_project: str, flow_name: str) -> int:
        session_id = self.get_session_id()
        params = {
            'session.id': session_id,
            'project': az_project,
            'ajax': 'fetchFlowExecutions',
            'flow': flow_name,
            'start': 0,
            'length': 1,
        }
        url = urllib.parse.urljoin(self.az_url, '/manager')
        self.async_client.push_get_http_param(url=url, params=params, retry=3)
        data = self.async_client.get()[0]
        if isinstance(data, dict) and 'error' not in data:
            exec_id = [execution.get('execId') for execution in data['executions']]
            if exec_id:
                return exec_id[0]
        else:
            self.log.debug(f'Following data received while getting the flow executions. Data: {data}')


    @staticmethod
    def _parse_and_get_current_flow_date(data: dict):
        current_flow_date_str = re.search('Current flow date:.*', data.get('data', ''))
        if current_flow_date_str:
            current_flow_date = re.search("\\d+", current_flow_date_str.group()).group()
            return current_flow_date
        current_flow_date_str = re.search('INFO\\s+flow_date=.*', data.get('data', ''))
        if current_flow_date_str:
            current_flow_date = re.search("\\d+", current_flow_date_str.group()).group()
            return current_flow_date

    def get_flow_date_and_execution_updates(self, exec_id: int, flow_name: str):
        session_id = self.get_session_id()
        url = urllib.parse.urljoin(self.az_url, '/executor')
        params = {
            'session.id': session_id,
            'ajax': 'fetchexecflowupdate',
            'execid': exec_id,
            'lastUpdateTime': -1
        }
        self.async_client.push_get_http_param(url=url, params=params, retry=3)
        jobId = f"{flow_name}_trigger"
        offset = 100000
        length = 100000
        params = {
            'session.id': session_id,
            'project': 'metricsV2',
            'ajax': 'fetchExecJobLogs',
            'execid': exec_id,
            'jobId': jobId,
            'offset': offset,
            'length': length
        }
        self.async_client.push_get_http_param(url=url, params=params, retry=3)
        data = self.async_client.get()

        output = {"flow_execution_updates": data.get(0, {}),
                  "flow_date": self._parse_and_get_current_flow_date(data.get(1, {}))
                  }
        return output

    @staticmethod
    def get_job_list_need_to_be_disabled(flow_date_and_execution_update: dict, az_project) -> list:
        if flow_date_and_execution_update.get('flow_execution_updates', {}).get('status') in ['SUCCEEDED', 'RUNNING']:
            return []
        disabled_jobs = {}
        failed_jobs = {}
        pinot_metadata_push_node = ''
        pinot_build_and_push_job_node = ''
        pinot_post_push_node = ''
        # print(json.dumps(flow_date_and_execution_update.get('flow_execution_updates', {}), indent=4))
        for job_node in flow_date_and_execution_update.get('flow_execution_updates', {}).get('nodes'):
            if job_node.get('status') in ('SKIPPED', 'SUCCEEDED'):
                if az_project == AzProjectName.METRICSV2.value:
                    if not (job_node.get('id').endswith('_trigger')
                            or job_node.get('id').endswith('_trigger_weekly')
                            or job_node.get('id').endswith('_trigger_monthly')
                            or job_node.get('id').endswith('_persist_metadata')
                            or job_node.get('id').endswith('_persist_metadata_weekly')
                            or job_node.get('id').endswith('_persist_metadata_monthly')
                            or job_node.get('id').endswith('_prune_dates')):
                        disabled_jobs[job_node.get('id')] = job_node
                elif az_project == AzProjectName.DIMENSIONS.value:
                    if not job_node.get('id').endswith('_trigger'):
                        disabled_jobs[job_node.get('id')] = job_node
            else:
                failed_jobs[job_node.get('id')] = job_node

        for pinot_additive_type in ('additive', 'non_additive'):
            for job_node in flow_date_and_execution_update.get('flow_execution_updates', {}).get('nodes'):
                if f'_pinot_metadata_push_{pinot_additive_type}' in job_node.get('id'):
                    pinot_metadata_push_node = job_node.get('id')
                elif f'_pinot_build_and_push_job_{pinot_additive_type}' in job_node.get('id'):
                    pinot_build_and_push_job_node = job_node.get('id')
                elif f'_pinot_post_push_{pinot_additive_type}' in job_node.get('id'):
                    pinot_post_push_node = job_node.get('id')
            if failed_jobs.get(pinot_post_push_node, {}).get('status') in ['FAILED', 'KILLED', 'CANCELLED']:
                disabled_jobs.pop(pinot_build_and_push_job_node, None)
                disabled_jobs.pop(pinot_metadata_push_node, None)
            elif failed_jobs.get(pinot_build_and_push_job_node, {}).get('status') in ['FAILED', 'KILLED', 'CANCELLED']:
                disabled_jobs.pop(pinot_metadata_push_node, None)
        # No else for the metadata_push job because it's the parent of the others, so none of these jobs would be in
        # the disable_map.
        return list(disabled_jobs)

    def execute_flow(self, disabled_jobs: list, az_project: str, flow_name: str, dry_run: bool = True,
                     flow_override: dict = None) -> dict:
        """
        This method executes the flow.
        :param flow_override: <dict> This is flowOverride parameters like flow_date or force_daily_flow etc
        :param disabled_jobs: <list> List of job that needs to be disabled.
        :param az_project: <str> azkaban project name
        :param flow_name: <str> flow name
        :param dry_run: <Boolean> flow that
        :return: <int> execid
        """
        session_id = self.get_session_id()
        url = urllib.parse.urljoin(self.az_url, '/executor')
        params = {
            'session.id': session_id,
            'ajax': 'executeFlow',
            'project': az_project,
            'flow': flow_name,
            'disabled': json.dumps(disabled_jobs),
        }
        if flow_override:
            for k, v in flow_override.items():
                params[f'flowOverride[{k}]'] = v

        if not dry_run:
            self.async_client.push_get_http_param(url=url, params=params, retry=3)
            data = self.async_client.get()[0]
            if 'error' in data:
                raise AzkabanError(data)
            # print(data.get('message', ''))
            return data
            # return data.get('execid', -1)
        else:
            params.pop('session.id', None)
            return {'message': f"Dryrun: execute flow parameters {json.dumps(params, indent=4)}"}


class ExecuteFlowError(Exception):
    pass


@dataclasses.dataclass
class ExecuteFlow:
    flow_name: str
    az_project: str
    cluster_name: str
    new_exec_id: int = dataclasses.field(init=False)
    old_exec_id: int = dataclasses.field(init=False)

    def __post_init__(self):
        self.new_exec_id = 0
        self.old_exec_id = 0
        if self.cluster_name == ClusterName.HOLDEM.value:
            self.az_client = Azkaban(AZUrls.UXPHOLDEM.value)
        elif self.cluster_name == ClusterName.WAR.value:
            self.az_client = Azkaban(AZUrls.UXPWAR.value)
        elif self.cluster_name == ClusterName.FARO.value:
            self.az_client = Azkaban(AZUrls.UXPFARO.value)
        else:
            raise ValueError("Invalid cluster name passed")
        if not hasattr(AzProjectName, self.az_project):
            raise ValueError("Invalid az project name passed")

    def restart_flow(self, dry_run: bool = True):
        self.old_exec_id = self.az_client.get_latest_flow_execution_id(az_project=self.az_project,
                                                                       flow_name=self.flow_name)
        if not self.old_exec_id:
            raise ExecuteFlowError(
                f"Did not found the execution id for az_project: {self.az_project}; "
                f"flow_name: {self.flow_name} cluster_name: {self.cluster_name}")

        flow_date_and_execution_updates = self.az_client.get_flow_date_and_execution_updates(exec_id=self.old_exec_id,
                                                                                             flow_name=self.flow_name
                                                                                             )

        if not flow_date_and_execution_updates.get('flow_execution_updates'):
            raise ExecuteFlowError(
                f"Flow execution updates not found for exec_id: {self.old_exec_id} and flowname: {self.flow_name}"
            )

        if not flow_date_and_execution_updates.get('flow_date'):
            raise ExecuteFlowError(
                f"flow_date not found for exec_id: {self.old_exec_id} and flowname: {self.flow_name}"
            )

        if flow_date_and_execution_updates.get('flow_execution_updates', {}).get('status') == 'SUCCEEDED':
            return f"Last execution id {self.old_exec_id} of flow {self.flow_name} has succeeded"

        elif flow_date_and_execution_updates.get('flow_execution_updates', {}).get('status') == 'RUNNING':
            return f"Last execution id {self.old_exec_id} of flow {self.flow_name} is running"

        elif flow_date_and_execution_updates.get('flow_execution_updates', {}).get('status') == 'PREPARING':
            return f"Last execution id {self.old_exec_id} of flow {self.flow_name} is preparing"

        job_list_need_to_be_disabled = self.az_client.get_job_list_need_to_be_disabled(
            flow_date_and_execution_update=flow_date_and_execution_updates,
            az_project=self.az_project
        )
        flow_override = {'flow_date': flow_date_and_execution_updates.get('flow_date')}

        for job_name in job_list_need_to_be_disabled:
            if job_name.endswith('_union'):
                flow_override['force_daily_flow'] = True

        response = self.az_client.execute_flow(disabled_jobs=job_list_need_to_be_disabled,
                                               az_project=self.az_project,
                                               flow_name=self.flow_name,
                                               dry_run=dry_run,
                                               flow_override=flow_override,
                                               )
        self.new_exec_id = response.get('execid', -1)
        return response.get('message', '')

