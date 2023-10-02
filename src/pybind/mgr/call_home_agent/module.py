"""
IBM Ceph Call Home Agent
Authors:
    Yaarit Hatuka <yhatuka@ibm.com>
    Juan Miguel Olmo Martinez <jolmomar@ibm.com>
"""

from typing import List, Any, Tuple, Dict, Optional, Set, Callable
import time
import json
import requests
import asyncio
import os
from datetime import datetime

from mgr_module import (Option, CLIReadCommand, CLIWriteCommand, MgrModule,
                        HandleCommandResult)
# from .dataClasses import ReportHeader, ReportEvent
from .dataDicts import ReportHeader, ReportEvent

class SendError(Exception):
    pass

def get_status(mgr: Any) -> dict:
    r, outb, outs = mgr.mon_command({
        'prefix': 'status',
        'format': 'json'
    })
    if r:
        error = f"status command failed: {outs}"
        mgr.log.error(error)
        return {'error': error}
    try:
        status_dict = json.loads(outb)
        status_dict["ceph_version"] = mgr.version
        status_dict["health_detail"] = json.loads(mgr.get('health')['json'])
        return status_dict
    except Exception as ex:
        mgr.log.exception(str(ex))
        return {'exception': str(ex)}

def inventory(mgr: Any) -> dict:
    """
    Produce the content for the inventory report

    Returns a dict with a json structure with the ceph cluster inventory information
    """
    inventory = {}
    inventory["crush_map"] = mgr.get("osd_map_crush")
    inventory["devices"] = mgr.get("devices")
    inventory["df"] = mgr.get("df")
    inventory["fs_map"] = mgr.get("fs_map")
    inventory["hosts"] = mgr.list_servers()
    inventory["manager_map"] = mgr.get("mgr_map")
    inventory["mon_map"] = mgr.get("mon_map")
    inventory["osd_map"] = mgr.get("osd_map")
    inventory["osd_metadata"] = mgr.get("osd_metadata")
    inventory["osd_tree"] = mgr.get("osd_map_tree")
    inventory["pg_summary"] = mgr.get("pg_summary")
    inventory["service_map"] = mgr.get("service_map")
    inventory["status"] = get_status(mgr)

    return {'inventory': inventory}

def performance(mgr: Any) -> dict:
    """
    Produce the content for the performance report

    Returns a dict with a json structure with the ceph cluster performance information
    """
    return {'performance': {'content': 'wathever'}}

def status(mgr: Any) -> dict:
    """
    Produce the content for the status report

    Returns a dict with a json structure with the ceph cluster health information
    """
    return {'status': get_status(mgr)}

def last_contact(mgr: Any) -> dict:
    """
    Produce the content for the last_contact report

    Returns a dict with just the timestamp of the last contact with the cluster
    """
    return {'last_contact': format(int(time.time()))}

class Report:
    def __init__(self, report_type: str, description: str, icn: str, owner_tenant_id: str, fn: Callable[[], str], url: str, proxy: str, seconds_interval: int,
                 mgr_module: Any):
        self.report_type = report_type                # name of the report
        self.icn = icn                                # ICN = IBM Customer Number
        self.owner_tenant_id = owner_tenant_id        # IBM tenant ID
        self.fn = fn                                  # function used to retrieve the data
        self.url = url                                # url to send the report
        self.interval = seconds_interval              # interval to send the report (seconds)
        self.mgr = mgr_module
        self.description = description
        self.last_id = ''
        self.proxies = {'http': proxy, 'https': proxy} if proxy else {}

        # Last upload settings
        self.last_upload_option_name = 'report_%s_last_upload' % self.report_type
        last_upload = self.mgr.get_store(self.last_upload_option_name, None)
        if last_upload is None:
            self.last_upload = str(int(time.time()) - self.interval + 1)
        else:
            self.last_upload = str(int(last_upload))

    def __str__(self) -> str:
        report = {}
        report_dt = datetime.timestamp(datetime.now())
        try:
            report = ReportHeader.collect(self.report_type,
                                  self.mgr.get('mon_map')['fsid'],
                                  self.mgr.version,
                                  report_dt,
                                  self.mgr,
                                  self.mgr.target_space)

            event_section = ReportEvent.collect(self.report_type,
                                        report_dt,
                                        self.mgr.get('mon_map')['fsid'],
                                        self.icn,
                                        self.owner_tenant_id,
                                        self.description,
                                        self.fn,
                                        self.mgr)

            report['events'].append(event_section)
            self.last_id = report["event_time_ms"]

            return json.dumps(report)
        except Exception as ex:
            raise Exception('<%s> report not available: %s\n%s' % (self.report_type, ex, report))

    def filter_report(self, fields_to_remove: list) -> str:
        filtered_report = json.loads(str(self))

        for field in fields_to_remove:
            if field in filtered_report:
                del filtered_report[field]

        return json.dumps(filtered_report)

    def send(self, force: bool = False) -> str:
        # Do not send report if the required interval is not reached
        if not force:
            if (int(time.time()) - int(self.last_upload)) < self.interval:
                self.mgr.log.info('%s report not sent because interval not reached', self.report_type)
                return ""
        resp = None
        try:
            if self.proxies:
                self.mgr.log.info('Sending <%s> report to <%s> (via proxies <%s>)', self.report_type, self.url,
                                  self.proxies)
            else:
                self.mgr.log.info('Sending <%s> report to <%s>', self.report_type, self.url)
            resp = requests.post(url=self.url,
                                 headers={'accept': 'application/json', 'content-type': 'application/json'},
                                 data=str(self),
                                 proxies=self.proxies)
            resp.raise_for_status()
            self.mgr.log.info(resp.json())
            self.last_upload = str(int(time.time()))
            self.mgr.set_store(self.last_upload_option_name, self.last_upload)
            self.mgr.health_checks.pop('CHA_ERROR_SENDING_REPORT', None)
            self.mgr.log.info('Successfully sent <%s> report(%s) to <%s>', self.report_type, self.last_id, self.url)
            return resp.text
        except Exception as e:
            explanation = resp.text if resp else ""
            raise SendError('Failed to send <%s> to <%s>: %s %s' % (self.report_type, self.url, str(e), explanation))

class CallHomeAgent(MgrModule):
    MODULE_OPTIONS: List[Option] = [
        Option(
            name='target',
            type='str',
            default='https://esupport.ibm.com/connect/api/v1',
            desc='Call Home endpoint'
        ),
        Option(
            name='interval_inventory_report_seconds',
            type='int',
            min=0,
            default=60 * 60 * 24,  # one day
            desc='Time frequency for the inventory report'
        ),
        Option(
            name='interval_performance_report_seconds',
            type='int',
            min=0,
            default=60 * 5,  # 5 minutes
            desc='Time frequency for the performance report'
        ),
        Option(
            name='interval_status_report_seconds',
            type='int',
            min=0,
            default=60 * 30,  # 30 minutes
            desc='Time frequency for the status report'
        ),
        Option(
            name='interval_last_contact_report_seconds',
            type='int',
            min=0,
            default=60 * 5,  # 5 minutes
            desc='Time frequency for the last contact report'
        ),
        Option(
            name='customer_email',
            type='str',
            default='',
            desc='Customer contact email'
        ),
        Option(
            name='icn',
            type='str',
            default='',
            desc='IBM Customer Number'
        ),
        Option(
            name='customer_first_name',
            type='str',
            default='',
            desc='Customer first name'
        ),
        Option(
            name='customer_last_name',
            type='str',
            default='',
            desc='Customer last name'
        ),
        Option(
            name='customer_phone',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_company_name',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_address',
            type='str',
            default='',
            desc='Customer address'
        ),
        Option(
            name='customer_country_code',
            type='str',
            default='',
            desc='Customer country code'
        ),
        Option(
            name='owner_tenant_id',
            type='str',
            default="",
            desc='IBM tenant Id for IBM Storage Insigths'
        ),
        Option(
            name='owner_ibm_id',
            type='str',
            default="",
            desc='IBM w3id identifier for IBM Storage Insights'
        ),
        Option(
            name='owner_company_name',
            type='str',
            default="",
            desc='User Company name for IBM storage Insights'
        ),
        Option(
            name='owner_first_name',
            type='str',
            default="",
            desc='User first name for IBM storage Insights'
        ),
        Option(
            name='owner_last_name',
            type='str',
            default="",
            desc='User last name for IBM storage Insights'
        ),
        Option(
            name='owner_email',
            type='str',
            default="",
            desc='User email for IBM storage Insights'
        ),
        Option(
            name='proxy',
            type='str',
            default='',
            desc='Proxy to reach Call Home endpoint'
        ),
        Option(
            name='target_space',
            type='str',
            default='prod',
            desc='Target space for reports (dev, staging or production)'
        ),
        Option(
            name='si_web_service_url',
            type='str',
            default='https://join.insights.ibm.com/api/v1/em-integration',
            desc='URL used to register Ceph cluster in SI (staging or production)'
        ),
        Option(
            name='valid_container_registry',
            type='str',
            default=r'^.+\.icr\.io$',
            desc='Container registry pattern for urls where cephadm credentials(JWT token) are valid'
        ),

    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(CallHomeAgent, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True

        # Module options
        self.refresh_options()

        # Health checks
        self.health_checks: Dict[str, Dict[str, Any]] = dict()

        # Coroutines management
        self.loop = asyncio.new_event_loop()  # type: ignore
        # Array to hold coroutines launched
        self.tasks = []

        # Prepare reports
        self.prepare_reports()

    def refresh_options(self):
        # Env vars (if they exist) have preference over module options
        self.cha_target_url = str(os.environ.get('CHA_TARGET', self.get_module_option('target')))

        self.interval_inventory_seconds = int(
            os.environ.get('CHA_INTERVAL_INVENTORY_REPORT_SECONDS',
                           self.get_module_option('interval_inventory_report_seconds')))  # type: ignore
        self.interval_performance_seconds = int(
            os.environ.get('CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS',
                           self.get_module_option('interval_performance_report_seconds')))  # type: ignore
        self.interval_status_seconds = int(
            os.environ.get('CHA_INTERVAL_STATUS_REPORT_SECONDS',
                           self.get_module_option('interval_status_report_seconds')))  # type: ignore
        self.interval_last_contact_seconds = int(
            os.environ.get('CHA_INTERVAL_LAST_CONTACT_REPORT_SECONDS',
                           self.get_module_option('interval_last_contact_report_seconds')))  # type: ignore
        self.proxy = str(os.environ.get('CHA_PROXY', self.get_module_option('proxy')))
        self.target_space = os.environ.get('CHA_TARGET_SPACE', self.get_module_option('target_space'))
        self.si_web_service_url = os.environ.get('CHA_SI_WEB_SERVICE_URL', self.get_module_option('si_web_service_url'))

        # Customer identifiers do not use environment vars to be set
        self.icn = self.get_module_option('icn')
        self.customer_email = self.get_module_option('customer_email')
        self.customer_first_name = self.get_module_option('customer_first_name')
        self.customer_last_name = self.get_module_option('customer_last_name')
        self.customer_phone = self.get_module_option('customer_phone')
        self.customer_company_name = self.get_module_option('customer_company_name')
        self.customer_address = self.get_module_option('customer_address')
        self.customer_country_code = self.get_module_option('customer_country_code')

        # Owner identifiers used in IBM storage insights do not use environment vars to be set
        self.owner_tenant_id = self.get_module_option('owner_tenant_id')
        self.owner_ibm_id = self.get_module_option('owner_ibm_id')
        self.owner_company_name = self.get_module_option('owner_company_name')
        self.owner_first_name = self.get_module_option('owner_first_name')
        self.owner_last_name = self.get_module_option('owner_last_name')
        self.owner_email = self.get_module_option('owner_email')

        # Other options not using env vars
        self.valid_container_registry = self.get_module_option('valid_container_registry')

    def prepare_reports(self):
        self.reports = {'inventory': Report('inventory',
                                            'Ceph cluster composition',
                                            self.icn,
                                            self.owner_tenant_id,
                                            inventory,
                                            self.cha_target_url,
                                            self.proxy,
                                            self.interval_inventory_seconds,
                                            self),
                        'status': Report('status',
                                         'Ceph cluster status and health',
                                         self.icn,
                                         self.owner_tenant_id,
                                         status,
                                         self.cha_target_url,
                                         self.proxy,
                                         self.interval_status_seconds,
                                         self),
                        'last_contact': Report('last_contact',
                                               'Last contact timestamps with Ceph cluster',
                                               self.icn,
                                               self.owner_tenant_id,
                                               last_contact,
                                               self.cha_target_url,
                                               self.proxy,
                                               self.interval_last_contact_seconds,
                                               self)
        }

    def config_notify(self) -> None:
        """
        This only affects changes in ceph config options.
        To change configuration using env. vars a restart of the module
        will be neeed or the change in one config option will refresh
        configuration coming from env vars
        """
        self.refresh_options()
        self.prepare_reports()
        self.clean_coroutines()
        self.launch_coroutines()

    async def control_task(self, seconds: int) -> None:
        """
            Coroutine to allow cancel and reconfigure coroutines in only 10s
        """
        try:
            while self.run:
                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            return

    async def report_task(self, report: Report) -> None:
        """
            Coroutine for sending the report passed as parameter
        """
        self.log.info('Launched task for <%s> report each %s seconds)', report.report_type, report.interval)

        try:
            while self.run:
                try:
                    report.send()
                except Exception as ex:
                    send_error = str(ex)
                    self.log.error(send_error)
                    self.health_checks.update({
                        'CHA_ERROR_SENDING_REPORT': {
                            'severity': 'error',
                            'summary': 'IBM Ceph Call Home Agent manager module: error sending <{}> report to '
                                    'endpoint {}'.format(report.report_type, self.cha_target_url),
                            'detail': [send_error]
                        }
                    })

                self.set_health_checks(self.health_checks)
                await asyncio.sleep(report.interval)
        except asyncio.CancelledError:
            return

    def launch_coroutines(self) -> None:
        """
         Launch module coroutines (reports or any other async task)
        """
        try:
            for report_name, report in self.reports.items():
                t = self.loop.create_task(self.report_task(report))
                self.tasks.append(t)
            # Create control task to allow to reconfigure reports in 10 seconds
            t = self.loop.create_task(self.control_task(10))
            self.tasks.append(t)
            # run the async loop
            self.loop.run_forever()
        except Exception as ex:
            if str(ex) != 'This event loop is already running':
                self.log.exception(str(ex))

    def serve(self) -> None:
        """
            - Launch coroutines for report tasks
        """
        self.log.info('Starting IBM Ceph Call Home Agent')

        # Launch coroutines for the reports
        self.launch_coroutines()

        self.log.info('Call home agent finished')

    def clean_coroutines(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Cleaning coroutines')
        for t in self.tasks:
            t.cancel()
        self.tasks = []

    def shutdown(self) -> None:
        self.log.info('Stopping IBM call home module')
        self.run = False
        self.clean_coroutines
        self.loop.stop()
        return super().shutdown()

    @CLIReadCommand('callhome stop')
    def stop_cmd(self) -> Tuple[int, str, str]:
        self.shutdown()
        return HandleCommandResult(stdout=f'Remember to disable the '
                                   'call home module')

    @CLIReadCommand('callhome show')
    def print_report_cmd(self, report_type: str) -> Tuple[int, str, str]:
        """
            Prints the report requested.
            Available reports: inventory, status, last_contact
            Example:
                ceph callhome show inventory

        """
        if report_type in self.reports.keys():
            return HandleCommandResult(stdout=f"{self.reports[report_type].filter_report(['api_key', 'private_key'])}")
        else:
            return HandleCommandResult(stderr='Unknown report type')

    @CLIReadCommand('callhome send')
    def send_report_cmd(self, report_type: str) -> Tuple[int, str, str]:
        """
            Command for sending the report requested.
            Available reports: inventory, status, last_contact
            Example:
                ceph callhome send inventory
        """
        try:
            if report_type in self.reports.keys():
                resp = self.reports[report_type].send(force=True)
            else:
                raise Exception('Unknown report type')
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=f'{report_type} report sent successfully:\n{resp}')


    @CLIReadCommand('callhome list-tenants')
    def list_tenants(self, owner_ibm_id: str, owner_company_name: str,
                       owner_first_name: str, owner_last_name: str,
                       owner_email: str) -> Tuple[int, str, str]:
        """
        Retrieves the list of tenant ids linked with an specific IBM id owner
        """
        mon_map = self.get('mon_map')
        mon_ips = ','.join([mon['addr'] for mon in mon_map['mons']])
        owner_data = {'owner-ibm-id': owner_ibm_id,
                'company-name': owner_company_name,
                'owner-first-name': owner_first_name,
                'owner-last-name': owner_last_name,
                'owner-email': owner_email,
                'check-only': True,
                'device-serial': mon_map['fsid'],
                'device-IP': mon_ips
                }

        resp = None
        try:
            resp = requests.post(url=self.si_web_service_url,
                                headers={'accept': 'application/json',
                                        'content-type': 'application/json',
                                        'IBM-SRM-SenderApp': 'CEPH-EM',
                                        'IBM-SRM-Request': 'SI-SignUp-Check'},
                                data=json.dumps(owner_data),
                                proxies=self.proxy)

            resp.raise_for_status()
        except Exception as ex:
            explanation = resp.text if resp else str(ex)
            self.log.error(explanation)
            return HandleCommandResult(stderr=explanation)
        else:
            return HandleCommandResult(stdout=f'{json.dumps(resp.json())}')

    @CLIWriteCommand('callhome set tenant')
    def set_tenant_id(self, owner_tenant_id: str, owner_ibm_id: str,
                      owner_company_name: str, owner_first_name: str,
                      owner_last_name: str, owner_email: str) -> Tuple[int, str, str]:
        """
        Set the IBM tenant id included in reports sent to IBM Storage Insights
        """
        try:
            self.set_module_option('owner_tenant_id', owner_tenant_id)
            self.set_module_option('owner_ibm_id', owner_ibm_id)
            self.set_module_option('owner_company_name', owner_company_name)
            self.set_module_option('owner_first_name', owner_first_name)
            self.set_module_option('owner_last_name', owner_last_name)
            self.set_module_option('owner_email', owner_email)
            self.prepare_reports()
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=f'IBM tenant id set to {owner_tenant_id}')

    @CLIReadCommand('callhome show user info')
    def customer(self) ->  Tuple[int, str, str]:
        """
        Show the information about the customer used to identify the customer
        in IBM call home and IBM storage insights systems
        """
        return HandleCommandResult(stdout=json.dumps(
            {'IBM_call_home': {
                    'icn': self.icn,
                    'first_name': self.customer_first_name,
                    'last_name': self.customer_last_name,
                    'phone': self.customer_phone,
                    'address': self.customer_address,
                    'email': self.customer_email,
                    'company': self.customer_company_name,
                    'country code': self.customer_country_code
                },
             'IBM_storage_insights': {
                    'owner_ibm_id': self.owner_ibm_id,
                    'owner_company_name': self.owner_company_name,
                    'owner_first_name': self.owner_first_name,
                    'owner_last_name': self.owner_last_name,
                    'owner_email': self.owner_email,
                    'owner_IBM_tenant_id': self.owner_tenant_id
                },
            }))
