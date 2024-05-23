import errno
import logging
import json
from typing import List, cast, Optional
from ipaddress import ip_address, IPv6Address

from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import NvmeofServiceSpec

from orchestrator import OrchestratorError, DaemonDescription, DaemonDescriptionStatus
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class NvmeofService(CephService):
    TYPE = 'nvmeof'
    PROMETHEUS_PORT = 10008

    def config(self, spec: NvmeofServiceSpec) -> bool:  # type: ignore
        assert self.TYPE == spec.service_type
        if not spec.pool:
            self.mgr.log.error(f"nvmeof config pool should be defined: {spec.pool}")
            return False
        if spec.group is None:
            self.mgr.log.error(f"nvmeof config group should not be None: {spec.group}")
            return False
        # unlike some other config funcs, if this fails we can't
        # go forward deploying the daemon and then retry later. For
        # that reason we make no attempt to catch the OrchestratorError
        # this may raise
        self.mgr._check_pool_exists(spec.pool, spec.service_name())
        return True

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        nvmeof_gw_id = daemon_spec.daemon_id
        host_ip = self.mgr.inventory.get_addr(daemon_spec.host)

        keyring = self.get_keyring_with_caps(self.get_auth_entity(nvmeof_gw_id),
                                             ['mon', 'profile rbd',
                                              'osd', 'profile rbd'])

        # TODO: check if we can force jinja2 to generate dicts with double quotes instead of using json.dumps
        transport_tcp_options = json.dumps(spec.transport_tcp_options) if spec.transport_tcp_options else None
        name = '{}.{}'.format(utils.name_to_config_section('nvmeof'), nvmeof_gw_id)
        rados_id = name[len('client.'):] if name.startswith('client.') else name
        context = {
            'spec': spec,
            'name': name,
            'addr': host_ip,
            'port': spec.port,
            'spdk_log_level': 'WARNING',
            'rpc_socket_dir': '/var/tmp/',
            'rpc_socket_name': 'spdk.sock',
            'transport_tcp_options': transport_tcp_options,
            'rados_id': rados_id
        }
        gw_conf = self.mgr.template.render('services/nvmeof/ceph-nvmeof.conf.j2', context)

        daemon_spec.keyring = keyring
        daemon_spec.extra_files = {'ceph-nvmeof.conf': gw_conf}

        if spec.enable_auth:
            if (
                not spec.client_cert
                or not spec.client_key
                or not spec.server_cert
                or not spec.server_key
            ):
                self.mgr.log.error(f'enable_auth set for {spec.service_name()} spec, but at '
                                   'least one of server/client cert/key fields missing. TLS '
                                   f'not being set up for {daemon_spec.name()}')
            else:
                daemon_spec.extra_files['server_cert'] = spec.server_cert
                daemon_spec.extra_files['client_cert'] = spec.client_cert
                daemon_spec.extra_files['server_key'] = spec.server_key
                daemon_spec.extra_files['client_key'] = spec.client_key

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        daemon_spec.deps = []
        if not hasattr(self, 'gws'):
            self.gws = {}  # id -> name map of gateways for this service.
        self.gws[nvmeof_gw_id] = name  # add to map of service's gateway names
        return daemon_spec

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]) -> None:
        """ Overrides the daemon_check_post to add nvmeof gateways safely
        """
        self.mgr.log.info(f"nvmeof daemon_check_post {daemon_descrs}")
        spec = cast(NvmeofServiceSpec,
                    self.mgr.spec_store.all_specs.get(daemon_descrs[0].service_name(), None))
        if not spec:
            self.mgr.log.error(f'Failed to find spec for {daemon_descrs[0].name()}')
            return
        pool = spec.pool
        group = spec.group
        for dd in daemon_descrs:
            self.mgr.log.info(f"nvmeof daemon_descr {dd}")
            if dd.daemon_id not in self.gws:
                err_msg = ('Trying to daemon_check_post nvmeof but daemon_id is unknown')
                logger.error(err_msg)
                raise OrchestratorError(err_msg)
            name = self.gws[dd.daemon_id]
            self.mgr.log.info(f"nvmeof daemon name={name}")
            # Notify monitor about this gateway creation
            cmd = {
                'prefix': 'nvme-gw create',
                'id': name,
                'group': group,
                'pool': pool
            }
            self.mgr.log.info(f"create gateway: monitor command {cmd}")
            _, _, err = self.mgr.mon_command(cmd)
            if err:
                err_msg = (f"Unable to send monitor command {cmd}, error {err}")
                logger.error(err_msg)
                raise OrchestratorError(err_msg)
        super().daemon_check_post(daemon_descrs)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        def get_set_cmd_dicts(out: str) -> List[dict]:
            gateways = json.loads(out)['gateways']
            cmd_dicts = []

            spec = cast(NvmeofServiceSpec,
                        self.mgr.spec_store.all_specs.get(daemon_descrs[0].service_name(), None))

            for dd in daemon_descrs:
                if dd.hostname is None:
                    err_msg = ('Trying to config_dashboard nvmeof but no hostname is defined')
                    logger.error(err_msg)
                    raise OrchestratorError(err_msg)

                if not spec:
                    logger.warning(f'No ServiceSpec found for {dd.service_name()}')
                    continue

                ip = utils.resolve_ip(self.mgr.inventory.get_addr(dd.hostname))
                if type(ip_address(ip)) is IPv6Address:
                    ip = f'[{ip}]'
                service_url = '{}:{}'.format(ip, spec.port or '5500')
                gw = gateways.get(dd.hostname)
                if not gw or gw['service_url'] != service_url:
                    logger.info(f'Adding NVMeoF gateway {service_url} to Dashboard')
                    cmd_dicts.append({
                        'prefix': 'dashboard nvmeof-gateway-add',
                        'inbuf': service_url,
                        'name': dd.hostname
                    })
            return cmd_dicts

        self._check_and_set_dashboard(
            service_name='nvmeof',
            get_cmd='dashboard nvmeof-gateway-list',
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 nvmeof, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Nvmeof', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nvmeof daemon. make sure none are down
        warn_message = ('ALERT: 1 nvmeof daemon is already down. Please bring it back up before stopping this one')
        nvmeof_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        for i in nvmeof_daemons:
            if i.status != DaemonDescriptionStatus.running:
                return HandleCommandResult(-errno.EBUSY, '', warn_message)

        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        warn_message = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, warn_message, '')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """
        logger.debug(f'Post remove daemon {self.TYPE}.{daemon.daemon_id}')
        # remove config for dashboard nvmeof gateways if any
        ret, out, err = self.mgr.mon_command({
            'prefix': 'dashboard nvmeof-gateway-rm',
            'name': daemon.hostname,
        })
        if not ret:
            logger.info(f'{daemon.hostname} removed from nvmeof gateways dashboard config')

        spec = cast(NvmeofServiceSpec,
                    self.mgr.spec_store.all_specs.get(daemon.service_name(), None))
        if not spec:
            self.mgr.log.error(f'Failed to find spec for {daemon.name()}')
            return
        pool = spec.pool
        group = spec.group

        if daemon.daemon_id not in self.gws:
            err_msg = (f'Trying to remove nvmeof but {daemon.daemon_id} '
                       'not in gws list')
            logger.error(err_msg)
            raise OrchestratorError(err_msg)
        name = self.gws[daemon.daemon_id]
        self.gws.pop(daemon.daemon_id)
        # Notify monitor about this gateway deletion
        cmd = {
            'prefix': 'nvme-gw delete',
            'id': name,
            'group': group,
            'pool': pool
        }
        self.mgr.log.info(f"delete gateway: monitor command {cmd}")
        _, _, err = self.mgr.mon_command(cmd)
        if err:
            self.mgr.log.error(f"Unable to send monitor command {cmd}, error {err}")

    def purge(self, service_name: str) -> None:
        """Make sure no zombie gateway is left behind
        """
        # Assert configured
        spec = cast(NvmeofServiceSpec, self.mgr.spec_store.all_specs.get(service_name, None))
        if not spec:
            self.mgr.log.error(f'Failed to find spec for {service_name}')
            return
        pool = spec.pool
        group = spec.group
        for daemon_id in self.gws:
            name = self.gws[daemon_id]
            self.gws.pop(daemon_id)
            # Notify monitor about this gateway deletion
            cmd = {
                'prefix': 'nvme-gw delete',
                'id': name,
                'group': group,
                'pool': pool
            }
            self.mgr.log.info(f"purge delete gateway: monitor command {cmd}")
            _, _, err = self.mgr.mon_command(cmd)
            if err:
                err_msg = (f'Monitor command "{cmd}" failed: {err}')
                logger.error(err_msg)
                raise OrchestratorError(err_msg)
