from typing import cast, List, Dict, Any, Optional, TYPE_CHECKING
from os.path import isabs
from enum import Enum

from mgr_module import NFS_GANESHA_SUPPORTED_FSALS

from ceph.utils import bytes_to_human, with_units_to_int
from .exception import NFSInvalidOperation, FSNotFound
from .utils import check_fs

if TYPE_CHECKING:
    from nfs.module import Module


def _indentation(depth: int, size: int = 4) -> str:
    return " " * (depth * size)


def _format_val(block_name: str, key: str, val: str) -> str:
    if isinstance(val, list):
        return ', '.join([_format_val(block_name, key, v) for v in val])
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, int) or (block_name == 'CLIENT'
                                and key == 'clients'):
        return '{}'.format(val)
    return '"{}"'.format(val)


def _validate_squash(squash: str) -> None:
    valid_squash_ls = [
        "root", "root_squash", "rootsquash", "rootid", "root_id_squash",
        "rootidsquash", "all", "all_squash", "allsquash", "all_anomnymous",
        "allanonymous", "no_root_squash", "none", "noidsquash",
    ]
    if squash.lower() not in valid_squash_ls:
        raise NFSInvalidOperation(
            f"squash {squash} not in valid list {valid_squash_ls}"
        )


def _validate_access_type(access_type: str) -> None:
    valid_access_types = ['rw', 'ro', 'none']
    if not isinstance(access_type, str) or access_type.lower() not in valid_access_types:
        raise NFSInvalidOperation(
            f'{access_type} is invalid, valid access type are'
            f'{valid_access_types}'
        )


def _validate_sec_type(sec_type: str) -> None:
    valid_sec_types = ["none", "sys", "krb5", "krb5i", "krb5p"]
    if not isinstance(sec_type, str) or sec_type not in valid_sec_types:
        raise NFSInvalidOperation(
            f"SecType {sec_type} invalid, valid types are {valid_sec_types}")


class RawBlock():
    def __init__(self, block_name: str, blocks: List['RawBlock'] = [], values: Dict[str, Any] = {}):
        if not values:  # workaround mutable default argument
            values = {}
        if not blocks:  # workaround mutable default argument
            blocks = []
        self.block_name = block_name
        self.blocks = blocks
        self.values = values

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RawBlock):
            return False
        return self.block_name == other.block_name and \
            self.blocks == other.blocks and \
            self.values == other.values

    def __repr__(self) -> str:
        return f'RawBlock({self.block_name!r}, {self.blocks!r}, {self.values!r})'


class GaneshaConfParser:
    def __init__(self, raw_config: str):
        self.pos = 0
        self.text = ""
        for line in raw_config.split("\n"):
            line = line.lstrip()

            if line.startswith("%"):
                self.text += line.replace('"', "")
                self.text += "\n"
            else:
                self.text += "".join(line.split())

    def stream(self) -> str:
        return self.text[self.pos:]

    def last_context(self) -> str:
        return f'"...{self.text[max(0, self.pos - 30):self.pos]}<here>{self.stream()[:30]}"'

    def parse_block_name(self) -> str:
        idx = self.stream().find('{')
        if idx == -1:
            raise Exception(f"Cannot find block name at {self.last_context()}")
        block_name = self.stream()[:idx]
        self.pos += idx + 1
        return block_name

    def parse_block_or_section(self) -> RawBlock:
        if self.stream().startswith("%url "):
            # section line
            self.pos += 5
            idx = self.stream().find('\n')
            if idx == -1:
                value = self.stream()
                self.pos += len(value)
            else:
                value = self.stream()[:idx]
                self.pos += idx + 1
            block_dict = RawBlock('%url', values={'value': value})
            return block_dict

        block_dict = RawBlock(self.parse_block_name().upper())
        self.parse_block_body(block_dict)
        if self.stream()[0] != '}':
            raise Exception("No closing bracket '}' found at the end of block")
        self.pos += 1
        return block_dict

    def parse_parameter_value(self, raw_value: str) -> Any:
        if raw_value.find(',') != -1:
            return [self.parse_parameter_value(v.strip())
                    for v in raw_value.split(',')]
        try:
            return int(raw_value)
        except ValueError:
            if raw_value == "true":
                return True
            if raw_value == "false":
                return False
            if raw_value.find('"') == 0:
                return raw_value[1:-1]
            return raw_value

    def parse_stanza(self, block_dict: RawBlock) -> None:
        equal_idx = self.stream().find('=')
        if equal_idx == -1:
            raise Exception("Malformed stanza: no equal symbol found.")
        semicolon_idx = self.stream().find(';')
        parameter_name = self.stream()[:equal_idx].lower()
        parameter_value = self.stream()[equal_idx + 1:semicolon_idx]
        block_dict.values[parameter_name] = self.parse_parameter_value(parameter_value)
        self.pos += semicolon_idx + 1

    def parse_block_body(self, block_dict: RawBlock) -> None:
        while True:
            if self.stream().find('}') == 0:
                # block end
                return

            last_pos = self.pos
            semicolon_idx = self.stream().find(';')
            lbracket_idx = self.stream().find('{')
            is_semicolon = (semicolon_idx != -1)
            is_lbracket = (lbracket_idx != -1)
            is_semicolon_lt_lbracket = (semicolon_idx < lbracket_idx)

            if is_semicolon and ((is_lbracket and is_semicolon_lt_lbracket) or not is_lbracket):
                self.parse_stanza(block_dict)
            elif is_lbracket and ((is_semicolon and not is_semicolon_lt_lbracket)
                                  or (not is_semicolon)):
                block_dict.blocks.append(self.parse_block_or_section())
            else:
                raise Exception("Malformed stanza: no semicolon found.")

            if last_pos == self.pos:
                raise Exception("Infinite loop while parsing block content")

    def parse(self) -> List[RawBlock]:
        blocks = []
        while self.stream():
            blocks.append(self.parse_block_or_section())
        return blocks


class FSAL(object):
    def __init__(self, name: str, cmount_path: Optional[str] = "/") -> None:
        # By default, cmount_path is set to "/", allowing the export to mount at the root level.
        # This ensures that the export path can be any complete path hierarchy within the Ceph filesystem.
        # If multiple exports share the same cmount_path and FSAL options, they will share a single CephFS client.
        self.name = name
        self.cmount_path = cmount_path

    @classmethod
    def from_dict(cls, fsal_dict: Dict[str, Any]) -> 'FSAL':
        if fsal_dict.get('name') == NFS_GANESHA_SUPPORTED_FSALS[0]:
            return CephFSFSAL.from_dict(fsal_dict)
        if fsal_dict.get('name') == NFS_GANESHA_SUPPORTED_FSALS[1]:
            return RGWFSAL.from_dict(fsal_dict)
        raise NFSInvalidOperation(f'Unknown FSAL {fsal_dict.get("name")}')

    @classmethod
    def from_fsal_block(cls, fsal_block: RawBlock) -> 'FSAL':
        if fsal_block.values.get('name') == NFS_GANESHA_SUPPORTED_FSALS[0]:
            return CephFSFSAL.from_fsal_block(fsal_block)
        if fsal_block.values.get('name') == NFS_GANESHA_SUPPORTED_FSALS[1]:
            return RGWFSAL.from_fsal_block(fsal_block)
        raise NFSInvalidOperation(f'Unknown FSAL {fsal_block.values.get("name")}')

    def to_fsal_block(self) -> RawBlock:
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError


class CephFSFSAL(FSAL):
    def __init__(self,
                 name: str,
                 user_id: Optional[str] = None,
                 fs_name: Optional[str] = None,
                 sec_label_xattr: Optional[str] = None,
                 cephx_key: Optional[str] = None,
                 cmount_path: Optional[str] = "/") -> None:
        super().__init__(name)
        assert name == 'CEPH'
        self.cmount_path = cmount_path
        self.fs_name = fs_name
        self.user_id = user_id
        self.sec_label_xattr = sec_label_xattr
        self.cephx_key = cephx_key

    @classmethod
    def from_fsal_block(cls, fsal_block: RawBlock) -> 'CephFSFSAL':
        return cls(fsal_block.values['name'],
                   fsal_block.values.get('user_id'),
                   fsal_block.values.get('filesystem'),
                   fsal_block.values.get('sec_label_xattr'),
                   fsal_block.values.get('secret_access_key'),
                   cmount_path=fsal_block.values.get('cmount_path'))

    def to_fsal_block(self) -> RawBlock:
        result = RawBlock('FSAL', values={'name': self.name})

        if self.user_id:
            result.values['user_id'] = self.user_id
        if self.fs_name:
            result.values['filesystem'] = self.fs_name
        if self.sec_label_xattr:
            result.values['sec_label_xattr'] = self.sec_label_xattr
        if self.cephx_key:
            result.values['secret_access_key'] = self.cephx_key
        if self.cmount_path:
            result.values['cmount_path'] = self.cmount_path
        return result

    @classmethod
    def from_dict(cls, fsal_dict: Dict[str, Any]) -> 'CephFSFSAL':
        return cls(fsal_dict['name'],
                   fsal_dict.get('user_id'),
                   fsal_dict.get('fs_name'),
                   fsal_dict.get('sec_label_xattr'),
                   fsal_dict.get('cephx_key'),
                   fsal_dict.get('cmount_path'))

    def to_dict(self) -> Dict[str, str]:
        r = {'name': self.name}
        if self.user_id:
            r['user_id'] = self.user_id
        if self.fs_name:
            r['fs_name'] = self.fs_name
        if self.sec_label_xattr:
            r['sec_label_xattr'] = self.sec_label_xattr
        if self.cmount_path:
            r['cmount_path'] = self.cmount_path
        return r


class RGWFSAL(FSAL):
    def __init__(self,
                 name: str,
                 user_id: Optional[str] = None,
                 access_key_id: Optional[str] = None,
                 secret_access_key: Optional[str] = None
                 ) -> None:
        super().__init__(name)
        assert name == 'RGW'
        # RGW user uid
        self.user_id = user_id
        # S3 credentials
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @classmethod
    def from_fsal_block(cls, fsal_block: RawBlock) -> 'RGWFSAL':
        return cls(fsal_block.values['name'],
                   fsal_block.values.get('user_id'),
                   fsal_block.values.get('access_key_id'),
                   fsal_block.values.get('secret_access_key'))

    def to_fsal_block(self) -> RawBlock:
        result = RawBlock('FSAL', values={'name': self.name})

        if self.user_id:
            result.values['user_id'] = self.user_id
        if self.access_key_id:
            result.values['access_key_id'] = self.access_key_id
        if self.secret_access_key:
            result.values['secret_access_key'] = self.secret_access_key
        return result

    @classmethod
    def from_dict(cls, fsal_dict: Dict[str, str]) -> 'RGWFSAL':
        return cls(fsal_dict['name'],
                   fsal_dict.get('user_id'),
                   fsal_dict.get('access_key_id'),
                   fsal_dict.get('secret_access_key'))

    def to_dict(self) -> Dict[str, str]:
        r = {'name': self.name}
        if self.user_id:
            r['user_id'] = self.user_id
        if self.access_key_id:
            r['access_key_id'] = self.access_key_id
        if self.secret_access_key:
            r['secret_access_key'] = self.secret_access_key
        return r


class QOSParams(Enum):
    clust_block = "QOS_DEFAULT_CONFIG"
    export_block = "QOS_BLOCK"
    enable_qos = "enable_qos"
    enable_bw_ctrl = "enable_bw_control"
    combined_bw_ctrl = "combined_rw_bw_control"
    qos_type = "qos_type"
    export_writebw = "max_export_write_bw"
    export_readbw = "max_export_read_bw"
    client_writebw = "max_client_write_bw"
    client_readbw = "max_client_read_bw"
    export_rw_bw = "max_export_combined_bw"
    client_rw_bw = "max_client_combined_bw"


class QOSType(Enum):
    _1 = 'PerShare'
    _2 = 'PerClient'
    _3 = 'PerShare_PerClient'


def _validate_qos_bw(bandwidth: str) -> int:
    min_bw = 1000000  # 1MB
    max_bw = 2000000000  # 2GB
    bw_bytes = with_units_to_int(bandwidth)
    if bw_bytes != 0 and (bw_bytes < min_bw or bw_bytes > max_bw):
        raise Exception(f"Provided bandwidth value is not in range, Please enter a value between {min_bw} and {max_bw} bytes")
    return bw_bytes


class QOS(object):
    def __init__(self,
                 cluster_op: bool = False,
                 enable_qos: bool = False,
                 enable_bw_ctrl: bool = False,
                 combined_bw_ctrl: bool = False,
                 qos_type: Optional[QOSType] = None,
                 export_writebw: str = '0',
                 export_readbw: str = '0',
                 client_writebw: str = '0',
                 client_readbw: str = '0',
                 export_rw_bw: str = '0',
                 client_rw_bw: str = '0'
                 ) -> None:
        self.cluster_op = cluster_op
        self.enable_qos = enable_qos
        self.enable_bw_ctrl = enable_bw_ctrl
        self.qos_type = qos_type
        self.combined_bw_ctrl = combined_bw_ctrl
        try:
            self.export_writebw: int = _validate_qos_bw(export_writebw)
            self.export_readbw: int = _validate_qos_bw(export_readbw)
            self.client_writebw: int = _validate_qos_bw(client_writebw)
            self.client_readbw: int = _validate_qos_bw(client_readbw)
            self.export_rw_bw: int = _validate_qos_bw(export_rw_bw)
            self.client_rw_bw: int = _validate_qos_bw(client_rw_bw)
        except Exception as e:
            raise Exception(f"Invalid bandwidth value. {e}")

    @classmethod
    def from_dict(cls, qos_dict: Dict[str, Any], cluster_op: bool = False) -> 'QOS':
        kwargs: dict[str, Any] = {}
        # qos dict will have qos type as enum value(str) and bandwidths in human redable format(str)
        if cluster_op:
            qos_type = qos_dict.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs['qos_type'] = QOSType(qos_type)
        kwargs.update(
            {
                'enable_qos': qos_dict.get(QOSParams.enable_qos.value),
                'enable_bw_ctrl': qos_dict.get(QOSParams.enable_bw_ctrl.value),
                'combined_bw_ctrl': qos_dict.get(QOSParams.combined_bw_ctrl.value),
                'export_writebw': qos_dict.get(QOSParams.export_writebw.value, 0),
                'export_readbw': qos_dict.get(QOSParams.export_readbw.value, 0),
                'client_writebw': qos_dict.get(QOSParams.client_writebw.value, 0),
                'client_readbw': qos_dict.get(QOSParams.client_readbw.value, 0),
                'export_rw_bw': qos_dict.get(QOSParams.export_rw_bw.value, 0),
                'client_rw_bw': qos_dict.get(QOSParams.client_rw_bw.value, 0)
            }
        )
        return cls(cluster_op, **kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock, cluster_op: bool = False) -> 'QOS':
        kwargs: dict[str, Any] = {}
        # qos block will have qos type as enum name without underscore(int) and bandwidths in bytes(int)
        if cluster_op:
            qos_type = qos_block.values.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs['qos_type'] = QOSType[f"_{qos_type}"]
        kwargs.update(
            {
                'enable_qos': qos_block.values.get(QOSParams.enable_qos.value),
                'enable_bw_ctrl': qos_block.values.get(QOSParams.enable_bw_ctrl.value),
                'combined_bw_ctrl': qos_block.values.get(QOSParams.combined_bw_ctrl.value),
                'export_writebw': str(qos_block.values.get(QOSParams.export_writebw.value, 0)),
                'export_readbw': str(qos_block.values.get(QOSParams.export_readbw.value, 0)),
                'client_writebw': str(qos_block.values.get(QOSParams.client_writebw.value, 0)),
                'client_readbw': str(qos_block.values.get(QOSParams.client_readbw.value, 0)),
                'export_rw_bw': str(qos_block.values.get(QOSParams.export_rw_bw.value, 0)),
                'client_rw_bw': str(qos_block.values.get(QOSParams.client_rw_bw.value, 0))
            }
        )
        return cls(cluster_op, **kwargs)

    def to_qos_block(self) -> RawBlock:
        if self.cluster_op:
            result = RawBlock(QOSParams.clust_block.value)
        else:
            result = RawBlock(QOSParams.export_block.value)
        # qos block will have qos type as enum name without underscore(int) and bandwidths in bytes(int)
        result.values[QOSParams.enable_qos.value] = self.enable_qos
        result.values[QOSParams.enable_bw_ctrl.value] = self.enable_bw_ctrl
        result.values[QOSParams.combined_bw_ctrl.value] = self.combined_bw_ctrl
        if self.cluster_op:
            if self.qos_type:
                result.values[QOSParams.qos_type.value] = int(self.qos_type.name[1])
        if self.export_writebw:
            result.values[QOSParams.export_writebw.value] = self.export_writebw
        if self.export_readbw:
            result.values[QOSParams.export_readbw.value] = self.export_readbw
        if self.client_writebw:
            result.values[QOSParams.client_writebw.value] = self.client_writebw
        if self.client_readbw:
            result.values[QOSParams.client_readbw.value] = self.client_readbw
        if self.export_rw_bw:
            result.values[QOSParams.export_rw_bw.value] = self.export_rw_bw
        if self.client_rw_bw:
            result.values[QOSParams.client_rw_bw.value] = self.client_rw_bw
        return result

    def to_dict(self) -> Dict[str, Any]:
        r: Dict[str, Any] = {}
        # qos dict will have qos type as enum value(str) and bandwidths in human redable format(str)
        r[QOSParams.enable_qos.value] = self.enable_qos
        r[QOSParams.enable_bw_ctrl.value] = self.enable_bw_ctrl
        r[QOSParams.combined_bw_ctrl.value] = self.combined_bw_ctrl
        if self.cluster_op:
            if self.qos_type:
                r[QOSParams.qos_type.value] = self.qos_type.value
        if self.export_writebw:
            r[QOSParams.export_writebw.value] = bytes_to_human(self.export_writebw)
        if self.export_readbw:
            r[QOSParams.export_readbw.value] = bytes_to_human(self.export_readbw)
        if self.client_writebw:
            r[QOSParams.client_writebw.value] = bytes_to_human(self.client_writebw)
        if self.client_readbw:
            r[QOSParams.client_readbw.value] = bytes_to_human(self.client_readbw)
        if self.export_rw_bw:
            r[QOSParams.export_rw_bw.value] = bytes_to_human(self.export_rw_bw)
        if self.client_rw_bw:
            r[QOSParams.client_rw_bw.value] = bytes_to_human(self.client_rw_bw)
        return r

    def update_bandwidths(self,
                          export_writebw: str,
                          export_readbw: str,
                          client_writebw: str,
                          client_readbw: str,
                          export_rw_bw: str,
                          client_rw_bw: str) -> None:
        try:
            self.export_writebw = _validate_qos_bw(export_writebw)
            self.export_readbw = _validate_qos_bw(export_readbw)
            self.client_writebw = _validate_qos_bw(client_writebw)
            self.client_readbw = _validate_qos_bw(client_readbw)
            self.export_rw_bw = _validate_qos_bw(export_rw_bw)
            self.client_rw_bw = _validate_qos_bw(client_rw_bw)
        except Exception as e:
            raise Exception(f"Invalid bandwidth value. {e}")


class Client:
    def __init__(self,
                 addresses: List[str],
                 access_type: str,
                 squash: str):
        self.addresses = addresses
        self.access_type = access_type
        self.squash = squash

    @classmethod
    def from_client_block(cls, client_block: RawBlock) -> 'Client':
        addresses = client_block.values.get('clients', [])
        if isinstance(addresses, str):
            addresses = [addresses]
        return cls(addresses,
                   client_block.values.get('access_type', None),
                   client_block.values.get('squash', None))

    def to_client_block(self) -> RawBlock:
        result = RawBlock('CLIENT', values={'clients': self.addresses})
        if self.access_type:
            result.values['access_type'] = self.access_type
        if self.squash:
            result.values['squash'] = self.squash
        return result

    @classmethod
    def from_dict(cls, client_dict: Dict[str, Any]) -> 'Client':
        return cls(client_dict['addresses'], client_dict['access_type'],
                   client_dict['squash'])

    def to_dict(self) -> Dict[str, Any]:
        return {
            'addresses': self.addresses,
            'access_type': self.access_type,
            'squash': self.squash
        }


class Export:
    def __init__(
            self,
            export_id: int,
            path: str,
            cluster_id: str,
            pseudo: str,
            access_type: str,
            squash: str,
            security_label: bool,
            protocols: List[int],
            transports: List[str],
            fsal: FSAL,
            clients: Optional[List[Client]] = None,
            sectype: Optional[List[str]] = None,
            qos_block: Optional[QOS] = None) -> None:
        self.export_id = export_id
        self.path = path
        self.fsal = fsal
        self.cluster_id = cluster_id
        self.pseudo = pseudo
        self.access_type = access_type
        self.squash = squash
        self.attr_expiration_time = 0
        self.security_label = security_label
        self.protocols = protocols
        self.transports = transports
        self.clients: List[Client] = clients or []
        self.sectype = sectype
        self.qos_block = qos_block

    @classmethod
    def from_export_block(cls, export_block: RawBlock, cluster_id: str) -> 'Export':
        fsal_blocks = [b for b in export_block.blocks
                       if b.block_name == "FSAL"]

        client_blocks = [b for b in export_block.blocks
                         if b.block_name == "CLIENT"]

        qos_block = [b for b in export_block.blocks
                     if b.block_name == "qos_block"]
        qos_block = QOS.from_qos_block(qos_block[0]) if qos_block else None

        protocols = export_block.values.get('protocols')
        if not isinstance(protocols, list):
            protocols = [protocols]

        transports = export_block.values.get('transports')
        if isinstance(transports, str):
            transports = [transports]
        elif not transports:
            transports = []

        # if this module wrote the ganesha conf the param is camelcase
        # "SecType".  but for compatiblity with manually edited ganesha confs,
        # accept "sectype" too.
        sectype = (export_block.values.get("SecType")
                   or export_block.values.get("sectype") or None)
        return cls(export_block.values['export_id'],
                   export_block.values['path'],
                   cluster_id,
                   export_block.values['pseudo'],
                   export_block.values.get('access_type', 'none'),
                   export_block.values.get('squash', 'no_root_squash'),
                   export_block.values.get('security_label', True),
                   protocols,
                   transports,
                   FSAL.from_fsal_block(fsal_blocks[0]),
                   [Client.from_client_block(client)
                    for client in client_blocks],
                   sectype=sectype,
                   qos_block=qos_block
                   )

    def to_export_block(self) -> RawBlock:
        values = {
            'export_id': self.export_id,
            'path': self.path,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'attr_expiration_time': self.attr_expiration_time,
            'security_label': self.security_label,
            'protocols': self.protocols,
            'transports': self.transports,
        }
        if self.sectype:
            values['SecType'] = self.sectype
        result = RawBlock("EXPORT", values=values)
        result.blocks = [
            self.fsal.to_fsal_block()
        ] + [
            client.to_client_block()
            for client in self.clients
        ]
        if self.qos_block:
            result.blocks.append(self.qos_block.to_qos_block())
        return result

    @classmethod
    def from_dict(cls, export_id: int, ex_dict: Dict[str, Any]) -> 'Export':
        if ex_dict.get('qos_block'):
            qos_block = QOS.from_dict(ex_dict.get('qos_block', {}))
        else:
            qos_block = None
        return cls(export_id,
                   ex_dict.get('path', '/'),
                   ex_dict['cluster_id'],
                   ex_dict['pseudo'],
                   ex_dict.get('access_type', 'RO'),
                   ex_dict.get('squash', 'no_root_squash'),
                   ex_dict.get('security_label', True),
                   ex_dict.get('protocols', [3, 4]),
                   ex_dict.get('transports', ['TCP']),
                   FSAL.from_dict(ex_dict.get('fsal', {})),
                   [Client.from_dict(client) for client in ex_dict.get('clients', [])],
                   sectype=ex_dict.get("sectype"),
                   qos_block=qos_block
                   )

    def to_dict(self) -> Dict[str, Any]:
        values = {
            'export_id': self.export_id,
            'path': self.path,
            'cluster_id': self.cluster_id,
            'pseudo': self.pseudo,
            'access_type': self.access_type,
            'squash': self.squash,
            'security_label': self.security_label,
            'protocols': sorted([p for p in self.protocols]),
            'transports': sorted([t for t in self.transports]),
            'fsal': self.fsal.to_dict(),
            'clients': [client.to_dict() for client in self.clients]
        }
        if self.sectype:
            values['sectype'] = self.sectype
        if self.qos_block:
            values['qos_block'] = self.qos_block.to_dict()
        return values

    def validate(self, mgr: 'Module') -> None:
        if not isabs(self.pseudo) or self.pseudo == "/":
            raise NFSInvalidOperation(
                f"pseudo path {self.pseudo} is invalid. It should be an absolute "
                "path and it cannot be just '/'."
            )

        _validate_squash(self.squash)
        _validate_access_type(self.access_type)

        if not isinstance(self.security_label, bool):
            raise NFSInvalidOperation('security_label must be a boolean value')

        for p in self.protocols:
            if p not in [3, 4]:
                raise NFSInvalidOperation(f"Invalid protocol {p}")

        valid_transport = ["UDP", "TCP"]
        for trans in self.transports:
            if trans.upper() not in valid_transport:
                raise NFSInvalidOperation(f'{trans} is not a valid transport protocol')

        for client in self.clients:
            if client.squash:
                _validate_squash(client.squash)
            if client.access_type:
                _validate_access_type(client.access_type)

        if self.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[0]:
            fs = cast(CephFSFSAL, self.fsal)
            if not fs.fs_name or not check_fs(mgr, fs.fs_name):
                raise FSNotFound(fs.fs_name)
        elif self.fsal.name == NFS_GANESHA_SUPPORTED_FSALS[1]:
            rgw = cast(RGWFSAL, self.fsal)  # noqa
            pass
        else:
            raise NFSInvalidOperation('FSAL {self.fsal.name} not supported')

        for st in (self.sectype or []):
            _validate_sec_type(st)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Export):
            return False
        return self.to_dict() == other.to_dict()


def _format_block_body(block: RawBlock, depth: int = 0) -> str:
    conf_str = ""
    for blo in block.blocks:
        conf_str += format_block(blo, depth)

    for key, val in block.values.items():
        if val is not None:
            conf_str += _indentation(depth)
            fval = _format_val(block.block_name, key, val)
            conf_str += '{} = {};\n'.format(key, fval)
    return conf_str


def format_block(block: RawBlock, depth: int = 0) -> str:
    """Format a raw block object into text suitable as a ganesha configuration
    block.
    """
    if block.block_name == "%url":
        return '%url "{}"\n\n'.format(block.values['value'])

    conf_str = ""
    conf_str += _indentation(depth)
    conf_str += format(block.block_name)
    conf_str += " {\n"
    conf_str += _format_block_body(block, depth + 1)
    conf_str += _indentation(depth)
    conf_str += "}\n"
    return conf_str


QOS_REQ_PARAMS = {
    'combined_bw_disabled': {
        'PerShare': ['max_export_write_bw', 'max_export_read_bw'],
        'PerClient': ['max_client_write_bw', 'max_client_read_bw'],
        'PerShare_PerClient': ['max_export_write_bw', 'max_export_read_bw', 'max_client_write_bw', 'max_client_read_bw']
    },
    'combined_bw_enabled': {
        'PerShare': ['max_export_combined_bw'],
        'PerClient': ['max_client_combined_bw'],
        'PerShare_PerClient': ['max_export_combined_bw', 'max_client_combined_bw'],
    }
}


def qos_bandwidth_checks(qos_type: QOSType, combined_bw_ctrl: bool, **kwargs: Any) -> None:
    """Checks for enabling qos"""
    if not combined_bw_ctrl:
        req_params = QOS_REQ_PARAMS['combined_bw_disabled'][qos_type.value]
    else:
        req_params = QOS_REQ_PARAMS['combined_bw_enabled'][qos_type.value]
    if any((key in req_params and kwargs[key] == '0')
           or (key not in req_params and kwargs[key] != '0') for key in kwargs):
        raise Exception(f"When combined_rw_bw is {'enabled' if combined_bw_ctrl else 'disabled'} and qos_type is {qos_type.value}, only the following parameters are required: {','.join(req_params)}.")
