# Integrate with the kubernetes events API. 
# This module sends events to Kubernetes, and also captures/tracks all events
# in the rook-ceph namespace so kubernetes activity like pod restarts,
# imagepulls etc can be seen from within the ceph cluster itself.
#
# To interact with the events API, the mgr service to access needs to be 
# granted additional permissions
# e.g. kubectl -n rook-ceph edit clusterrole rook-ceph-mgr-cluster-rules
#
# These are the changes needed;
# - apiGroups:
#   - ""
#   resources:
#   - events
#   verbs:
#   - create
#   - patch
#   - list
#   - get
#   - watch


import os
import sys
import time
import json
import logging
import threading

from datetime import datetime, timedelta, timezone
from urllib3.exceptions import MaxRetryError
from collections import OrderedDict

import rados
from mgr_module import MgrModule

try:
    import queue
except ImportError:
    # python 2.7.5
    import Queue as queue
finally:
    # python 2.7.15 or python3
    event_queue = queue.Queue()

try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    kubernetes_imported = False
    client = None
    config = None
else:
    kubernetes_imported = True
    # Apply the kubernetes config from the cluster 
    config.load_incluster_config()

    # The watch.Watch.stream method can provide event objects that have involved_object = None
    # which causes an exception in the generator. A workaround is discussed for a similar issue
    # in https://github.com/kubernetes-client/python/issues/376 which has been used here
    # pylint: disable=no-member
    from kubernetes.client.models.v1_event import V1Event
    def local_involved_object(self, involved_object):
        if involved_object is None:
            involved_object = client.V1ObjectReference(api_version="1")
        self._involved_object = involved_object
    V1Event.involved_object = V1Event.involved_object.setter(local_involved_object)

log = logging.getLogger(__name__)

def text_suffix(num):
    """Define a text suffix based on a value i.e. turn host into hosts"""
    return '' if num == 1 else 's'


class HealthCheck(object):
    """Transform a healthcheck msg into it's component parts"""

    def __init__(self, msg, msg_level):

        # msg looks like
        #
        # Health check failed: Reduced data availability: 100 pgs inactive (PG_AVAILABILITY)
        # Health check cleared: OSDMAP_FLAGS (was: nodown flag(s) set)
        # Health check failed: nodown flag(s) set (OSDMAP_FLAGS)
        #
        self.msg = None
        self.name = None
        self.text = None
        self.valid = False
        
        if msg.lower().startswith('health check'):

            self.valid = True
            self.msg = msg
            msg_tokens = self.msg.split()
            
            if msg_level == 'INF':
                self.text = ' '.join(msg_tokens[3:])
                self.name = msg_tokens[3]   # health check name e.g. OSDMAP_FLAGS
            else:   # WRN or ERR
                self.text = ' '.join(msg_tokens[3:-1])
                self.name = msg_tokens[-1][1:-1]


class LogEntry(object):
    """Generic 'log' object"""

    reason_map = {
        "audit": "Audit",
        "cluster": "HealthCheck",
        "config": "ClusterChange",
        "heartbeat":"Heartbeat",
        "startup": "Started"
    }

    def __init__(self, source, msg, msg_type, level, tstamp=None):

        self.source = source
        self.msg = msg
        self.msg_type = msg_type
        self.level = level
        self.tstamp = tstamp
        self.healthcheck = None
        
        if 'health check ' in self.msg.lower():
            self.healthcheck = HealthCheck(self.msg, self.level)

    
    def __str__(self):
        return "source={}, msg_type={}, msg={}, level={}, tstamp={}".format(self.source,self.msg_type,self.msg,self.level,self.tstamp)

    @property
    def cmd(self):
        """Look at the msg string and extract the command content"""

        # msg looks like 'from=\'client.205306 \' entity=\'client.admin\' cmd=\'[{"prefix": "osd set", "key": "nodown"}]\': finished'
        if self.msg_type != 'audit':
            return None
        else:
            _m=self.msg[:-10].replace("\'","").split("cmd=")
            _s='"cmd":{}'.format(_m[1])
            cmds_list = json.loads('{' + _s + '}')['cmd']

            # TODO. Assuming only one command was issued for now
            _c = cmds_list[0]
            return "{} {}".format(_c['prefix'], _c.get('key', ''))
    
    @property
    def event_type(self):
        return 'Normal' if self.level == 'INF' else 'Warning'
    
    @property
    def event_reason(self):
        return self.reason_map[self.msg_type]

    @property
    def event_name(self):
        if self.msg_type == 'heartbeat':
            return 'mgr.Heartbeat'
        elif self.healthcheck:
            return 'mgr.health.{}'.format(self.healthcheck.name)
        elif self.msg_type == 'audit':
            return 'mgr.audit.{}'.format(self.cmd).replace(' ', '_')
        elif self.msg_type == 'config':
            return 'mgr.ConfigurationChange'
        elif self.msg_type == 'startup':
            return "mgr.k8sevents-module"
        else:
            return None
   
    @property
    def event_entity(self):
        if self.msg_type == 'audit':
            return self.msg.replace("\'","").split('entity=')[1].split(' ')[0]
        else:
            return None
    
    @property
    def event_msg(self):
        if self.msg_type == 'audit':
            return "Client '{}' issued: ceph {}".format(self.event_entity, self.cmd)

        elif self.healthcheck:
            return self.healthcheck.text
        else:
            return self.msg

class RookCeph(object):
    """Establish environment defaults when interacting with rook-ceph"""

    pod_name = os.environ['POD_NAME']
    host = os.environ['NODE_NAME']
    namespace = os.environ.get('POD_NAMESPACE', 'rook-ceph')
    cluster_name = os.environ.get('ROOK_CEPH_CLUSTER_CRD_NAME', 'rook-ceph')
    api = client.CoreV1Api()

class BaseThread(threading.Thread):
    health = 'OK'
    reported = False
    daemon = True


class NamespaceWatcher(RookCeph, BaseThread):
    """Watch events in a given namespace 
    
    Using the watch package we can listen to event traffic in the namespace to 
    get an idea of what kubernetes related events surround the ceph cluster. The
    thing to bear in mind is that events have a TTL enforced by the kube-apiserver
    so this stream will only really show activity inside this retention window.
    """

    def __init__(self, namespace=None):
        super(NamespaceWatcher, self).__init__()
        if namespace:                       # override the default
            self.namespace = namespace
        self.events = OrderedDict()
        self.lock = threading.Lock()
        self.active = None
        self.resource_version = None

    def fetch(self):
        # clear the cache on every call to fetch
        self.events.clear()
        try:
            resp = self.api.list_namespaced_event(self.namespace)
        # TODO - Perhaps test for auth problem to be more specific in the except clause?
        except:
            self.active = False
            self.health = "Unable to access events API (list_namespaced_event call failed)"
            log.warning(self.health)
        else:
            self.active = True
            self.resource_version = resp.metadata.resource_version
            
            for item in resp.items:
                self.events[item.metadata.name] = item
            log.info('Added {} events'.format(len(resp.items)))

    def run(self):
        self.fetch()
        func = getattr(self.api, "list_namespaced_event")

        if self.active:
            log.info("Namespace event watcher started")

            
            while True:

                try:
                    w = watch.Watch()
                    # execute generator to continually watch resource for changes
                    for item in w.stream(func, namespace=self.namespace, resource_version=self.resource_version, watch=True):
                        obj = item['object']

                        with self.lock:

                            if item['type'] in ['ADDED', 'MODIFIED']:
                                self.events[obj.metadata.name] = obj

                            elif item['type'] == 'DELETED':
                                del self.events[obj.metadata.name]
                            
                # TODO test the exception for auth problem (403?)
    
                # Attribute error is generated when urllib3 on the system is old and doesn't have a
                # read_chunked method
                except AttributeError as e:
                    self.health = ("Unable to 'watch' events API in namespace {} - "
                                "incompatible urllib3? ({})".format(self.namespace, e))
                    self.active = False
                    log.warning(self.health)
                    break

                except ApiException as e:
                    # refresh the resource_version & watcher
                    log.warning("API exception caught in watcher ({})".format(e))
                    log.info("Restarting namespace watcher")
                    self.fetch()

                except Exception:
                    self.health = "{} Exception at {}".format(
                        sys.exc_info()[0].__name__,
                        datetime.strftime(datetime.now(),"%Y/%m/%d %H:%M:%S")
                    )
                    log.exception(self.health)
                    self.active = False
                    break

            log.warning("Namespace event watcher stopped")


class KubernetesEvent(RookCeph):

    def __init__(self, log_entry, unique_name=True):
        super(KubernetesEvent, self).__init__()

        self.event_name = log_entry.event_name
        self.message = log_entry.event_msg
        self.event_type = log_entry.event_type
        self.event_reason = log_entry.event_reason
        self.unique_name = unique_name

        self.api_status = 200
        self.count = 1
        self.first_timestamp = None
        self.last_timestamp = None

    @property
    def type(self):
        """provide a type property matching a V1Event object"""
        return self.event_type

    @property
    def event_body(self):
        if self.unique_name:
            obj_meta = client.V1ObjectMeta(name="{}".format(self.event_name)) 
        else:
            obj_meta = client.V1ObjectMeta(generate_name="{}".format(self.event_name))

        # field_path is needed to prevent problems in the namespacewatcher when
        # deleted event are received
        obj_ref = client.V1ObjectReference(kind="CephCluster",
                                           field_path='spec.containers{mgr}', 
                                           name=self.event_name, 
                                           namespace=self.namespace)

        event_source = client.V1EventSource(component="ceph-mgr", 
                                            host=self.host)
        return  client.V1Event(
                    involved_object=obj_ref, 
                    metadata=obj_meta, 
                    message=self.message, 
                    count=self.count, 
                    type=self.event_type,
                    reason=self.event_reason,
                    source=event_source, 
                    first_timestamp=self.first_timestamp,
                    last_timestamp=self.last_timestamp
                )

    def write(self):

        now=datetime.now(timezone.utc)

        self.first_timestamp = now
        self.last_timestamp = now

        try:
            self.api.create_namespaced_event(self.namespace, self.event_body)
        except MaxRetryError:
            # k8s config has not be defined properly 
            self.api_status = 403       # Forbidden
        except ApiException as e:
            self.api_status = e.status
            if e.status == 409:
                # 409 means the event is there already, so read it back (v1Event object returned)
                # this could happen if the event has been created, and then the k8sevent module
                # disabled and reenabled - i.e. the internal event tracking no longer matches k8s
                response = self.api.read_namespaced_event(self.event_name, self.namespace)
                #
                # response looks like
                #
                # {'action': None,
                # 'api_version': 'v1',
                # 'count': 1,
                # 'event_time': None,
                # 'first_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                # 'involved_object': {'api_version': None,
                #                     'field_path': None,
                #                     'kind': 'CephCluster',
                #                     'name': 'ceph-mgr.k8sevent-module',
                #                     'namespace': 'rook-ceph',
                #                     'resource_version': None,
                #                     'uid': None},
                # 'kind': 'Event',
                # 'last_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                # 'message': 'Ceph log -> event tracking started',
                # 'metadata': {'annotations': None,
                #             'cluster_name': None,
                #             'creation_timestamp': datetime.datetime(2019, 7, 18, 5, 24, 59, tzinfo=tzlocal()),
                #             'deletion_grace_period_seconds': None,
                #             'deletion_timestamp': None,
                #             'finalizers': None,
                #             'generate_name': 'ceph-mgr.k8sevent-module',
                #             'generation': None,
                #             'initializers': None,
                #             'labels': None,
                #             'name': 'ceph-mgr.k8sevent-module5z7kq',
                #             'namespace': 'rook-ceph',
                #             'owner_references': None,
                #             'resource_version': '1195832',
                #             'self_link': '/api/v1/namespaces/rook-ceph/events/ceph-mgr.k8sevent-module5z7kq',
                #             'uid': '62fde5f1-a91c-11e9-9c80-6cde63a9debf'},
                # 'reason': 'Started',
                # 'related': None,
                # 'reporting_component': '',
                # 'reporting_instance': '',
                # 'series': None,
                # 'source': {'component': 'ceph-mgr', 'host': 'minikube'},
                # 'type': 'Normal'}

                # conflict event already exists
                # read it
                # update : count and last_timestamp and msg

                self.count = response.count + 1
                self.first_timestamp = response.first_timestamp
                try:
                    self.api.patch_namespaced_event(self.event_name, self.namespace, self.event_body)
                except ApiException as e:
                    self.api_status = e.status
                else:
                    self.api_status = 200

        else:
            self.api_status = 200

    @property
    def api_success(self):
        return self.api_status == 200

    def update(self, log_entry):
        self.message = log_entry.event_msg
        self.event_type = log_entry.event_type
        self.last_timestamp = datetime.now(timezone.utc)
        self.count += 1

        try:
            self.api.patch_namespaced_event(self.event_name, self.namespace, self.event_body)
        except ApiException as e:
            if e.status == 404:
                # tried to patch, but hit a 404. The event's TTL must have been reached, and 
                # pruned by the kube-apiserver
                try:
                    self.api.create_namespaced_event(self.namespace, self.event_body)
                except ApiException as e:
                    self.api_status = e.status
                else:
                    self.api_status = 200
        else:
            self.api_status = 200 


class EventProcessor(BaseThread):
    """Handle a global queue used to track events we want to send/update to kubernetes"""

    can_run = True

    def __init__(self, config_watcher, event_retention_days):
        super(EventProcessor, self).__init__()
        self.events = dict()
        self.config_watcher = config_watcher
        self.event_retention_days = event_retention_days

    def startup(self):
        """Log an event to show we're active"""
        
        event = KubernetesEvent(
            LogEntry(
                source='self',
                msg='Ceph log -> event tracking started',
                msg_type='startup',
                level='INF',
                tstamp=None
            ),
            unique_name=False
        )
        
        event.write()
        return event.api_success
    
    @property
    def ok(self):
        return self.startup()

    def prune_events(self):

        oldest = datetime.now(timezone.utc) - timedelta(days=self.event_retention_days)
        local_events = dict(self.events)

        for event_name in sorted(local_events,
                                 key = lambda name: local_events[name].last_timestamp):
            event = local_events[event_name]
            if event.last_timestamp >= oldest:
                break
            else:
                # drop this event
                log.debug("Removing old event : {}".format(event_name))
                del self.events[event_name]

    def process(self, log_object):
        
        log.debug("K8sevents processing : {}".format(str(log_object)))

        event_out = False
        unique_name = True

        if log_object.msg_type == 'audit':
            # audit traffic : operator commands
            if log_object.msg.endswith('finished'):
                log.debug("K8sevents received command finished msg")
                event_out = True
            else:
                # NO OP - ignoring 'dispatch' log records
                return
        
        elif log_object.msg_type == 'cluster':
            # cluster messages : health checks
            if log_object.event_name:
                event_out = True

        elif log_object.msg_type == 'config':
            # configuration checker messages 
            event_out = True
            unique_name = False
            
        elif log_object.msg_type == 'heartbeat':
            # hourly health message summary from Ceph
            event_out = True
            unique_name = False
            log_object.msg = str(self.config_watcher)

        else:
            log.warning("K8sevents received unknown msg_type - {}".format(log_object.msg_type))

        if event_out:
            # we don't cache non-unique events like heartbeats or config changes
            if not unique_name or log_object.event_name not in self.events.keys():
                event = KubernetesEvent(log_entry=log_object,
                                        unique_name=unique_name)
                event.write()
                log.debug("event(unique={}) creation ended : {}".format(unique_name, event.api_status))
                if event.api_success and unique_name:
                    self.events[log_object.event_name] = event
            else:
                event = self.events[log_object.event_name]
                event.update(log_object)
                log.debug("Event update ended : {}".format(event.api_status))

            self.prune_events()

        else:
            log.debug("K8sevents ignored message : {}".format(log_object.msg))

    def run(self):
        log.info("Ceph event processing thread started, event retention set to {} days".format(self.event_retention_days))

        while True:

            try:
                log_object = event_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                try:
                    self.process(log_object)
                except Exception:
                    self.health = "{} Exception at {}".format(
                        sys.exc_info()[0].__name__,
                        datetime.strftime(datetime.now(),"%Y/%m/%d %H:%M:%S")
                    )
                    log.exception(self.health)
                    break
            
            if not self.can_run:
                break

            time.sleep(0.5)

        log.warning("Ceph event processing thread stopped")


class ListDiff(object):
    def __init__(self, before, after):
        self.before = set(before)
        self.after = set(after)

    @property
    def removed(self):
        return list(self.before - self.after)

    @property
    def added(self):
        return list(self.after - self.before)

    @property
    def is_equal(self):
        return self.before == self.after


class CephConfigWatcher(BaseThread):
    """Detect configuration changes within the cluster and generate human readable events"""

    def __init__(self, mgr):
        super(CephConfigWatcher, self).__init__()
        self.mgr = mgr
        self.server_map = dict()
        self.osd_map = dict()
        self.pool_map = dict()
        self.service_map = dict()
        
        self.config_check_secs = mgr.config_check_secs
    
    @property
    def raw_capacity(self):
        # Note. if the osd's are not online the capacity field will be 0
        return sum([self.osd_map[osd]['capacity'] for osd in self.osd_map])
    
    @property
    def num_servers(self):
        return len(self.server_map.keys())

    @property
    def num_osds(self):
        return len(self.osd_map.keys())
    
    @property
    def num_pools(self):
        return len(self.pool_map.keys())

    def __str__(self):
        s = ''

        s += "{} : {:>3} host{}, {} pool{}, {} OSDs. Raw Capacity {}B".format(
            json.loads(self.mgr.get('health')['json'])['status'],
            self.num_servers, 
            text_suffix(self.num_servers),
            self.num_pools, 
            text_suffix(self.num_pools),
            self.num_osds, 
            MgrModule.to_pretty_iec(self.raw_capacity))
        return s

    def fetch_servers(self):
        """Return a server summary, and service summary"""
        servers = self.mgr.list_servers()
        server_map = dict()         # host -> services
        service_map = dict()        # service -> host
        for server_info in servers:
            services = dict()
            for svc in server_info['services']:
                if svc.get('type') in services.keys():
                    services[svc.get('type')].append(svc.get('id'))
                else:
                    services[svc.get('type')] = list([svc.get('id')])
                # maintain the service xref map service -> host and version
                service_map[(svc.get('type'), str(svc.get('id')))] = server_info.get('hostname', '')
            server_map[server_info.get('hostname')] = services

        return server_map, service_map
    
    def fetch_pools(self):
        interesting = ["type", "size", "min_size"]
        # pools = [{'pool': 1, 'pool_name': 'replicapool', 'flags': 1, 'flags_names': 'hashpspool', 
        #           'type': 1, 'size': 3, 'min_size': 1, 'crush_rule': 1, 'object_hash': 2, 'pg_autoscale_mode': 'warn', 
        #           'pg_num': 100, 'pg_placement_num': 100, 'pg_placement_num_target': 100, 'pg_num_target': 100, 'pg_num_pending': 100, 
        #           'last_pg_merge_meta': {'ready_epoch': 0, 'last_epoch_started': 0, 'last_epoch_clean': 0, 'source_pgid': '0.0', 
        #           'source_version': "0'0", 'target_version': "0'0"}, 'auid': 0, 'snap_mode': 'selfmanaged', 'snap_seq': 0, 'snap_epoch': 0,
        #           'pool_snaps': [], 'quota_max_bytes': 0, 'quota_max_objects': 0, 'tiers': [], 'tier_of': -1, 'read_tier': -1, 
        #           'write_tier': -1, 'cache_mode': 'none', 'target_max_bytes': 0, 'target_max_objects': 0, 
        #           'cache_target_dirty_ratio_micro': 400000, 'cache_target_dirty_high_ratio_micro': 600000, 
        #           'cache_target_full_ratio_micro': 800000, 'cache_min_flush_age': 0, 'cache_min_evict_age': 0, 
        #           'erasure_code_profile': '', 'hit_set_params': {'type': 'none'}, 'hit_set_period': 0, 'hit_set_count': 0, 
        #           'use_gmt_hitset': True, 'min_read_recency_for_promote': 0, 'min_write_recency_for_promote': 0, 
        #           'hit_set_grade_decay_rate': 0, 'hit_set_search_last_n': 0, 'grade_table': [], 'stripe_width': 0, 
        #           'expected_num_objects': 0, 'fast_read': False, 'options': {}, 'application_metadata': {'rbd': {}}, 
        #           'create_time': '2019-08-02 02:23:01.618519', 'last_change': '19', 'last_force_op_resend': '0', 
        #           'last_force_op_resend_prenautilus': '0', 'last_force_op_resend_preluminous': '0', 'removed_snaps': '[]'}]
        pools = self.mgr.get('osd_map')['pools']
        pool_map = dict()
        for pool in pools:
            pool_map[pool.get('pool_name')] = {k:pool.get(k) for k in interesting}
        return pool_map


    def fetch_osd_map(self, service_map):
        """Create an osd map"""
        stats = self.mgr.get('osd_stats')

        osd_map = dict()

        devices = self.mgr.get('osd_map_crush')['devices']
        for dev in devices:
            osd_id = str(dev['id']) 
            osd_map[osd_id] = dict(
                deviceclass=dev.get('class'),
                capacity=0,
                hostname=service_map['osd', osd_id]
                )
        
        for osd_stat in stats['osd_stats']:
            osd_id = str(osd_stat.get('osd'))
            osd_map[osd_id]['capacity'] = osd_stat['statfs']['total']

        return osd_map

    def push_events(self, changes):
        """Add config change to the global queue to generate an event in kubernetes"""
        log.debug("{} events will be generated")
        for change in changes:
            event_queue.put(change)

    def _generate_config_logentry(self, msg):
        return LogEntry(
                source="config",
                msg_type="config",
                msg=msg,
                level='INF',
                tstamp=None
        )
    
    def _check_hosts(self, server_map):
        log.debug("K8sevents checking host membership")
        changes = list()
        servers = ListDiff(self.server_map.keys(), server_map.keys())
        if servers.is_equal:
            # no hosts have been added or removed
            pass
        else:
            # host changes detected, find out what
            host_msg = "Host '{}' has been {} the cluster"
            for new_server in servers.added:
                changes.append(self._generate_config_logentry(
                                    msg=host_msg.format(new_server, 'added to'))
                )

            for removed_server in servers.removed:
                changes.append(self._generate_config_logentry(
                                    msg=host_msg.format(removed_server, 'removed from'))
                )

        return changes

    def _check_osds(self,server_map, osd_map):
        log.debug("K8sevents checking OSD configuration")
        changes = list()
        before_osds = list()
        for svr in self.server_map:
            before_osds.extend(self.server_map[svr].get('osd',[]))

        after_osds = list()
        for svr in server_map:
            after_osds.extend(server_map[svr].get('osd',[]))
        
        if set(before_osds) == set(after_osds):
            # no change in osd id's
            pass
        else:
            # osd changes detected
            osd_msg = "Ceph OSD '{}' ({} @ {}B) has been {} host {}"

            osds = ListDiff(before_osds, after_osds)
            for new_osd in osds.added:
                changes.append(self._generate_config_logentry(
                                    msg=osd_msg.format(
                                            new_osd, 
                                            osd_map[new_osd]['deviceclass'],
                                            MgrModule.to_pretty_iec(osd_map[new_osd]['capacity']),
                                            'added to',
                                            osd_map[new_osd]['hostname']))
                )

            for removed_osd in osds.removed:
                changes.append(self._generate_config_logentry(
                                    msg=osd_msg.format(
                                        removed_osd,
                                        osd_map[removed_osd]['deviceclass'],
                                        MgrModule.to_pretty_iec(osd_map[removed_osd]['capacity']),
                                        'removed from',
                                        osd_map[removed_osd]['hostname']))
                )

        return changes

    def _check_pools(self, pool_map):
        changes = list()
        log.debug("K8sevents checking pool configurations")
        if self.pool_map.keys() == pool_map.keys():
            # no pools added/removed
            pass
        else:
            # Pool changes
            pools = ListDiff(self.pool_map.keys(), pool_map.keys())
            pool_msg = "Pool '{}' has been {} the cluster"
            for new_pool in pools.added:
                changes.append(self._generate_config_logentry(
                                    msg=pool_msg.format(new_pool, 'added to'))
                )

            for removed_pool in pools.removed:
                changes.append(self._generate_config_logentry(
                                    msg=pool_msg.format(removed_pool, 'removed from'))
                )

        # check pool configuration changes
        for pool_name in pool_map:
            if not self.pool_map.get(pool_name, dict()):
                # pool didn't exist before so just skip the checks
                continue

            if pool_map[pool_name] == self.pool_map[pool_name]:
                # no changes - dicts match in key and value
                continue
            else:
                # determine the change and add it to the change list
                size_diff = pool_map[pool_name]['size'] - self.pool_map[pool_name]['size']
                if size_diff != 0:
                    if size_diff < 0:
                        msg = "Data protection level of pool '{}' reduced to {} copies".format(pool_name,
                                                                                            pool_map[pool_name]['size'])
                        level = 'WRN'
                    else:
                        msg = "Data protection level of pool '{}' increased to {} copies".format(pool_name,
                                                                                                pool_map[pool_name]['size'])
                        level = 'INF'

                    changes.append(LogEntry(source="config",
                                msg_type="config",
                                msg=msg,
                                level=level,
                                tstamp=None)
                                )

                if pool_map[pool_name]['min_size'] != self.pool_map[pool_name]['min_size']:
                    changes.append(LogEntry(source="config",
                                msg_type="config",
                                msg="Minimum acceptable number of replicas in pool '{}' has changed".format(pool_name),
                                level='WRN',
                                tstamp=None)
                                )

        return changes

    def get_changes(self, server_map, osd_map, pool_map):
        """Detect changes in maps between current observation and the last"""

        changes = list()

        changes.extend(self._check_hosts(server_map))
        changes.extend(self._check_osds(server_map, osd_map))
        changes.extend(self._check_pools(pool_map))

        # FUTURE
        # Could generate an event if a ceph daemon has moved hosts
        # (assumes the ceph metadata host information is valid - which may not be the case)

        return changes

    def run(self):
        log.info("Ceph configuration watcher started, interval set to {}s".format(self.config_check_secs))

        self.server_map, self.service_map = self.fetch_servers()
        self.pool_map = self.fetch_pools()

        self.osd_map = self.fetch_osd_map(self.service_map)
        
        while True:

            try:
                start_time = time.time()
                server_map, service_map = self.fetch_servers()
                pool_map = self.fetch_pools()
                osd_map = self.fetch_osd_map(service_map)

                changes = self.get_changes(server_map, osd_map, pool_map)
                if changes:
                    self.push_events(changes)

                self.osd_map = osd_map
                self.pool_map = pool_map
                self.server_map = server_map
                self.service_map = service_map

                checks_duration = int(time.time() - start_time)
                
                # check that the time it took to run the checks fits within the 
                # interval, and if not extend the interval and emit a log message
                # to show that the runtime for the checks exceeded the desired 
                # interval
                if checks_duration > self.config_check_secs:
                    new_interval = self.config_check_secs * 2
                    log.warning("K8sevents check interval warning. "
                                "Current checks took {}s, interval was {}s. "
                                "Increasing interval to {}s".format(int(checks_duration),
                                                                   self.config_check_secs,
                                                                   new_interval))
                    self.config_check_secs = new_interval

                time.sleep(self.config_check_secs)    
            
            except Exception:
                self.health = "{} Exception at {}".format(
                    sys.exc_info()[0].__name__,
                    datetime.strftime(datetime.now(),"%Y/%m/%d %H:%M:%S")
                )
                log.exception(self.health)
                break

        log.warning("Ceph configuration watcher stopped")


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "k8sevents status",
            "desc": "Show the status of the data gathering threads",
            "perm": "r"
        },
        {
            "cmd": "k8sevents list",
            "desc": "List all current Kuberenetes events from the Ceph namespace",
            "perm": "r"
        },
        {
            "cmd": "k8sevents ceph",
            "desc": "List Ceph events tracked & sent to the kubernetes cluster",
            "perm": "r"
        }
    ]
    MODULE_OPTIONS = [
        {'name': 'config_check_secs',
         'type': 'int',
         'default': 10,
         'min': 10,
         'desc': "interval (secs) to check for cluster configuration changes"},
        {'name': 'ceph_event_retention_days',
         'type': 'int',
         'default': 7,
         'desc': "Days to hold ceph event information within local cache"}
    ]

    def __init__(self, *args, **kwargs):
        self.run = True
        self.event_processor = None
        self.config_watcher = None
        self.ns_watcher = None
        self.trackers = list()
        
        # Declare the module options we accept
        self.config_check_secs = None
        self.ceph_event_retention_days = None

        super(Module, self).__init__(*args, **kwargs)

    def config_notify(self):
        """Apply module options to runtime"""
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']) or opt['default'])

    def fetch_events(self, limit=None):
        """Interface to expose current events to another mgr module"""
        # FUTURE: Implement this to provide k8s events to the dashboard?
        raise NotImplementedError

    def process_clog(self, log_message):
        """Add log message to the event queue
        
        :param log_message:     dict from the cluster log (audit/cluster channels)
        """
        required_fields = ['channel', 'message', 'priority', 'stamp']
        _message_attrs = log_message.keys()
        if all(_field in _message_attrs for _field in required_fields):
            if log_message.get('message').startswith('overall HEALTH'):
                m_type = 'heartbeat'
            else:
                m_type = log_message.get('channel')

            event_queue.put(
                LogEntry(
                    source='log',
                    msg_type=m_type,
                    msg=log_message.get('message'),
                    level=log_message.get('priority')[1:-1],
                    tstamp=log_message.get('stamp')
                )
            )
            
        else:
            log.warning("Unexpected clog message format received - skipped: {}".format(log_message))

    def notify(self, notify_type, notify_id):
        """
        Called by the ceph-mgr service to notify the Python plugin
        that new state is available.

        :param notify_type: string indicating what kind of notification,
                            such as osd_map, mon_map, fs_map, mon_status,
                            health, pg_summary, command, service_map
        :param notify_id:  string (may be empty) that optionally specifies
                            which entity is being notified about.  With
                            "command" notifications this is set to the tag
                            ``from send_command``.
        """
        
        # only interested in cluster log messages for now
        if notify_type == 'clog':
            if isinstance(notify_id, dict):
                # create a log object to process
                self.process_clog(notify_id)
            else:
                log.warning("Expecting log record format of dict received {}".format(type(notify_type)))

    def _show_events(self, events):

        max_msg_length = max([len(events[k].message) for k in events])
        fmt = "{:<20}  {:<8}  {:>5}  {:<" + str(max_msg_length) + "}  {}\n"
        s = fmt.format("Last Seen", "Type", "Count", "Message", "Event Object Name")

        for event_name in sorted(events, 
                                 key = lambda name: events[name].last_timestamp,
                                 reverse=True):

            event = events[event_name]

            s += fmt.format(
                    datetime.strftime(event.last_timestamp,"%Y/%m/%d %H:%M:%S"),
                    event.type,
                    event.count,
                    event.message,
                    event_name
            )
        s += "Total : {:>3}\n".format(len(events))
        return s

    def show_events(self, events):
        """Show events we're holding from the ceph namespace - most recent 1st"""

        if len(events):
            return 0, "", self._show_events(events)
        else:
            return 0, "", "No events emitted yet, local cache is empty"

    def show_status(self):
        s = "Tracker Health\n"
        for t in self.trackers:
            s += "- {} : {}\n".format(t.__class__.__name__, t.health)
        s += "Tracked Events\n"
        s += "- namespace  : {:>3}\n".format(len(self.ns_watcher.events))
        s += "- ceph events: {:>3}\n".format(len(self.event_processor.events))
        return 0, "", s

    def handle_command(self, inbuf, cmd):
        # FUTURE: Should we implement dynamic options for the monitoring?
        if cmd["prefix"] == "k8sevents status":
            return self.show_status()
        elif cmd["prefix"] == "k8sevents list":
            return self.show_events(self.ns_watcher.events)
        elif cmd["prefix"] == "k8sevents ceph":
            return self.show_events(self.event_processor.events)
        else:
            raise NotImplementedError(cmd["prefix"])

    @staticmethod
    def can_run():
        """Determine whether the pre-reqs for the module are in place"""

        if not kubernetes_imported:
            return False, "kubernetes python client is unavailable"
        return True, ""

    def serve(self):
        self.config_notify()

        self.config_watcher = CephConfigWatcher(self)
        self.event_processor = EventProcessor(self.config_watcher, 
                                              self.ceph_event_retention_days)
        self.ns_watcher = NamespaceWatcher()

        if self.event_processor.ok:
            log.info("Ceph Log processor thread starting")
            self.event_processor.start()        # start log consumer thread
            log.info("Ceph config watcher thread starting")
            self.config_watcher.start()
            log.info("Rook-ceph namespace events watcher starting")
            self.ns_watcher.start()

            self.trackers.extend([self.event_processor, self.config_watcher, self.ns_watcher])
            
            while True:
                # stay alive
                time.sleep(1)

                trackers = self.trackers
                for t in trackers:
                    if not t.is_alive() and not t.reported:
                        log.error("K8sevents tracker thread '{}' stopped: {}".format(t.__class__.__name__, t.health))
                        t.reported = True

        else:
            log.warning('Unable to access kubernetes event API - check RBAC rules')
            log.warning("k8sevents module exiting")
            self.run = False

    def shutdown(self):
        self.run = False
        log.info("Shutting down k8sevents module")
        self.event_processor.can_run = False

        if self._rados:
            self._rados.shutdown()
