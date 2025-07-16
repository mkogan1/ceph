import logging
import threading
from typing import Tuple, Optional, List, Dict, Any
import yaml

from mgr_module import MgrModule, CLICommand, Option, CLICheckNonemptyFileInput
import object_format
import orchestrator
from orchestrator.module import IngressType
from mgr_util import CephFSEarmarkResolver

from .export import ExportMgr, AppliedExportResults
from .cluster import NFSCluster
from .utils import available_clusters

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS: List[Option] = []

    def __init__(self, *args: str, **kwargs: Any) -> None:
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        with self.lock:
            self.export_mgr = ExportMgr(self)
            self.nfs = NFSCluster(self)
            self.inited = True

    @CLICommand('nfs export create cephfs', perm='rw')
    @object_format.Responder()
    def _cmd_nfs_export_create_cephfs(
            self,
            cluster_id: str,
            pseudo_path: str,
            fsname: str,
            path: Optional[str] = '/',
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            squash: str = 'none',
            sectype: Optional[List[str]] = None,
            cmount_path: Optional[str] = "/",
            kmip_key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a CephFS export"""
        earmark_resolver = CephFSEarmarkResolver(self)
        return self.export_mgr.create_export(
            fsal_type='cephfs',
            fs_name=fsname,
            cluster_id=cluster_id,
            pseudo_path=pseudo_path,
            read_only=readonly,
            path=path,
            squash=squash,
            addr=client_addr,
            sectype=sectype,
            cmount_path=cmount_path,
            earmark_resolver=earmark_resolver,
            kmip_key_id=kmip_key_id
        )

    @CLICommand('nfs export create rgw', perm='rw')
    @object_format.Responder()
    def _cmd_nfs_export_create_rgw(
            self,
            cluster_id: str,
            pseudo_path: str,
            bucket: Optional[str] = None,
            user_id: Optional[str] = None,
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            squash: str = 'none',
            sectype: Optional[List[str]] = None,
            kmip_key_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an RGW export"""
        return self.export_mgr.create_export(
            fsal_type='rgw',
            bucket=bucket,
            user_id=user_id,
            cluster_id=cluster_id,
            pseudo_path=pseudo_path,
            read_only=readonly,
            squash=squash,
            addr=client_addr,
            sectype=sectype,
            kmip_key_id=kmip_key_id
        )

    @CLICommand('nfs export rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_export_rm(self, cluster_id: str, pseudo_path: str) -> None:
        """Remove a cephfs export"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export delete', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_export_delete(self, cluster_id: str, pseudo_path: str) -> None:
        """Delete a cephfs export (DEPRECATED)"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_ls(self, cluster_id: str, detailed: bool = False) -> List[Any]:
        """List exports of a NFS cluster"""
        return self.export_mgr.list_exports(cluster_id=cluster_id, detailed=detailed)

    @CLICommand('nfs export info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_info(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export get', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_get(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding (DEPRECATED)"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export apply', perm='rw')
    @CLICheckNonemptyFileInput(desc='Export JSON or Ganesha EXPORT specification')
    @object_format.Responder()
    def _cmd_nfs_export_apply(self, cluster_id: str, inbuf: str) -> AppliedExportResults:
        earmark_resolver = CephFSEarmarkResolver(self)
        """Create or update an export by `-i <json_or_ganesha_export_file>`"""
        return self.export_mgr.apply_export(cluster_id, export_config=inbuf,
                                            earmark_resolver=earmark_resolver)

    @CLICommand('nfs cluster create', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_create(self,
                                cluster_id: str,
                                placement: Optional[str] = None,
                                ingress: Optional[bool] = None,
                                virtual_ip: Optional[str] = None,
                                ingress_mode: Optional[IngressType] = None,
                                port: Optional[int] = None,
                                enable_virtual_server: bool = False,
                                inbuf: Optional[str] = None
                                ) -> None:
        """Create an NFS Cluster"""
        kmip_cert = kmip_key = kmip_ca_cert = kmip_host_list = None
        cluster_qos_config = None
        if inbuf:
            config = yaml.safe_load(inbuf)
            kmip_cert = config.get('kmip_cert')
            kmip_key = config.get('kmip_key')
            kmip_ca_cert = config.get('kmip_ca_cert')
            kmip_host_list = config.get('kmip_host_list')
            cluster_qos_config = config.get('cluster_qos_config')

        return self.nfs.create_nfs_cluster(cluster_id=cluster_id, placement=placement,
                                           virtual_ip=virtual_ip, ingress=ingress,
                                           ingress_mode=ingress_mode, port=port,
                                           enable_virtual_server=enable_virtual_server,
                                           kmip_cert=kmip_cert, kmip_key=kmip_key,
                                           kmip_ca_cert=kmip_ca_cert, kmip_host_list=kmip_host_list,
                                           cluster_qos_config=cluster_qos_config)

    @CLICommand('nfs cluster rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_rm(self, cluster_id: str) -> None:
        """Removes an NFS Cluster"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster delete', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_delete(self, cluster_id: str) -> None:
        """Removes an NFS Cluster (DEPRECATED)"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_ls(self) -> List[str]:
        """List NFS Clusters"""
        return self.nfs.list_nfs_cluster()

    @CLICommand('nfs cluster info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """Displays NFS Cluster info"""
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    @CLICommand('nfs cluster config get', perm='r')
    @object_format.ErrorResponseHandler()
    def _cmd_nfs_cluster_config_get(self, cluster_id: str) -> Tuple[int, str, str]:
        """Fetch NFS-Ganesha config"""
        conf = self.nfs.get_nfs_cluster_config(cluster_id=cluster_id)
        return 0, conf, ""

    @CLICommand('nfs cluster config set', perm='rw')
    @CLICheckNonemptyFileInput(desc='NFS-Ganesha Configuration')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_set(self, cluster_id: str, inbuf: str) -> None:
        """Set NFS-Ganesha config by `-i <config_file>`"""
        return self.nfs.set_nfs_cluster_config(cluster_id=cluster_id, nfs_config=inbuf)

    @CLICommand('nfs cluster config reset', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_reset(self, cluster_id: str) -> None:
        """Reset NFS-Ganesha Config to default"""
        return self.nfs.reset_nfs_cluster_config(cluster_id=cluster_id)

    def fetch_nfs_export_obj(self) -> ExportMgr:
        return self.export_mgr

    def export_ls(self, cluster_id: Optional[str] = None, detailed: bool = False) -> List[Dict[Any, Any]]:
        if not (cluster_id):
            return self.export_mgr.list_all_exports()
        return self.export_mgr.list_exports(cluster_id, detailed)

    def export_get(self, cluster_id: str, export_id: int) -> Optional[Dict[str, Any]]:
        return self.export_mgr.get_export_by_id(cluster_id, export_id)

    def export_rm(self, cluster_id: str, pseudo: str) -> None:
        self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo)

    def cluster_ls(self) -> List[str]:
        return available_clusters(self)

    def cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    def fetch_nfs_cluster_obj(self) -> NFSCluster:
        return self.nfs

    def enable_cluster_qos_bw(self, cluster_id: str,
                              qos_type: str,
                              combined_bw_ctrl: bool,
                              **kwargs: Any
                              ) -> None:
        qos = UserQoSType(qos_type)
        try:
            # by default it will take 0 which is expected by this function
            bw_obj = QOSBandwidthControl(enable_bw_ctrl=True,
                                         combined_bw_ctrl=combined_bw_ctrl,
                                         export_writebw=kwargs.get('max_export_write_bw', '0'),
                                         export_readbw=kwargs.get('max_export_read_bw', '0'),
                                         client_writebw=kwargs.get('max_client_write_bw', '0'),
                                         client_readbw=kwargs.get('max_client_read_bw', '0'),
                                         export_rw_bw=kwargs.get('max_export_combined_bw', '0'),
                                         client_rw_bw=kwargs.get('max_client_combined_bw', '0'))
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.nfs.enable_cluster_qos_bw(cluster_id=cluster_id,
                                              qos_type=QOSType[qos.value],
                                              bw_obj=bw_obj,
                                              skip_qos_type_validation=True)

    def enable_export_qos_bw(self, cluster_id: str,
                             pseudo_path: str,
                             combined_bw_ctrl: bool,
                             **kwargs: Any
                             ) -> None:
        try:
            bw_obj = QOSBandwidthControl(enable_bw_ctrl=True,
                                         combined_bw_ctrl=combined_bw_ctrl,
                                         export_writebw=kwargs.get('max_export_write_bw', '0'),
                                         export_readbw=kwargs.get('max_export_read_bw', '0'),
                                         client_writebw=kwargs.get('max_client_write_bw', '0'),
                                         client_readbw=kwargs.get('max_client_read_bw', '0'),
                                         export_rw_bw=kwargs.get('max_export_combined_bw', '0'),
                                         client_rw_bw=kwargs.get('max_client_combined_bw', '0'))
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.export_mgr.enable_export_qos_bw(cluster_id=cluster_id,
                                                    pseudo_path=pseudo_path,
                                                    bw_obj=bw_obj)

    def enable_cluster_qos_ops(self, cluster_id: str,
                               qos_type: str,
                               **kwargs: Any
                               ) -> None:
        qos = UserQoSType(qos_type)
        try:
            ops_obj = QOSOpsControl(enable_iops_ctrl=True,
                                    max_export_iops=kwargs.get('max_export_iops', 0),
                                    max_client_iops=kwargs.get('max_client_iops', 0))
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.nfs.enable_cluster_qos_ops(cluster_id=cluster_id,
                                               qos_type=QOSType[qos.value],
                                               ops_obj=ops_obj,
                                               skip_qos_type_validation=True)

    def enable_export_qos_ops(self, cluster_id: str,
                              pseudo_path: str,
                              **kwargs: Any
                              ) -> None:
        try:
            ops_obj = QOSOpsControl(enable_iops_ctrl=True,
                                    max_export_iops=kwargs.get('max_export_iops', 0),
                                    max_client_iops=kwargs.get('max_client_iops', 0))
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.export_mgr.enable_export_qos_ops(cluster_id=cluster_id,
                                                     pseudo_path=pseudo_path,
                                                     ops_obj=ops_obj)
