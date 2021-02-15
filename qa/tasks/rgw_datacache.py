"""
rgw datacache testing
"""
import argparse
import logging
import json

from io import BytesIO, StringIO
from configobj import ConfigObj
from teuthology.exceptions import ConfigError
from teuthology.task import Task
from teuthology import misc as teuthology
from teuthology.orchestra import run
from tasks.util.rgw import rgwadmin

log = logging.getLogger(__name__)
access_key = '0555b35654ad1656d704'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV7r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

class RGWDataCache(Task):
    """
    Runs a test against a rgw with the data cache enabled. A client must be
    set in the config for this task. This client must be the same client
    that is in the config for the rgw task.

    `datacache` and `datacache` must be configured for the rgw task and
    the ceph.conf must contain the below config variables in the client
    sections. `s3cmd` must be added as an extra_package to the install task.
    Ex:
    - overrides:
        rgw:
          datacache: true
          datacache_path: /tmp/rgw_datacache
        install:
          extra_packages:
            deb: ['s3cmd']
            rpm: ['s3cmd']
        ceph:
          conf:
            client:
              rgw d3n l1 datacache persistent path: /tmp/rgw_datacache/
              rgw d3n l1 datacache size: 10737417240
              rgw d3n l1 local datacache enabled: true
              rgw enable ops log: true
    - rgw:
        client.0:
    - rgw-datacache:
        client.0:
    """
    def __init__(self, ctx, config):
        super(RGWDataCache, self).__init__(ctx, config)
        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            raise ConfigError('no clients declared in rgw-datacache config')

    def setup(self):
        super(RGWDataCache, self).setup()

        if not self.ctx.rgw.datacache or self.ctx.rgw.datacache_path == None:
            raise ConfigError('rgw datacache not set up in the rgw task')

        self.ctx.rgw_datacache = argparse.Namespace()
        testdir = teuthology.get_testdir(self.ctx)
        self.ctx.rgw_datacache.archive = '{tdir}/archive'.format(tdir=testdir)
        self.ctx.rgw_datacache.s3cmd_cfg = '{tdir}/archive/s3cfg'.format(tdir=testdir)

        log.debug('RGW Datacache: SETUP BEGIN')

        for client in self.all_clients:
            file_path = '{path}/7M.dat'.format(path=self.ctx.rgw_datacache.archive)
            self.ctx.cluster.only(client).run(
                args=[
                    'dd',
                    'if=/dev/urandom',
                    'of={path}'.format(path=file_path),
                    'bs=1M',
                    'count=7',
                ],
                stdout=BytesIO()
            )

            # create rgw user for s3cmd operations
            display_name = 'rgw data cache user'
            email = 'rgw data_cache_user@email.com'
            uid = 'rgw_datacache_user'
            cmd = [
                'user',
                'create',
                '--uid', uid,
                '--display-name', display_name,
                '--access-key', access_key,
                '--secret', secret_key,
                '--email', email,
            ]
            log.info("RGW Datacache: Created S3 user")
            rgwadmin(self.ctx, client, cmd, check_status=True)

            # create minimal config for s3cmd
            self.create_s3cmd_config(client)

        log.debug('RGW Datacache: SETUP DONE')


    def begin(self):
        super(RGWDataCache, self).begin()
        log.debug('RGW Datacache: TEST BEGIN')

        for client in self.all_clients:
            # make bucket
            bucket_name = 'btk'
            mb_args = [
                'mb',
                's3://{bucket_name}'.format(bucket_name=bucket_name),
            ]
            self.run_s3cmd(client, mb_args)

            # put object in bucket
            put_file = '7M.dat'
            put_args = [
                'put',
                '{path}/{filename}'.format(path=self.ctx.rgw_datacache.archive,
                                           filename=put_file),
                's3://{bucket_name}'.format(bucket_name=bucket_name),
            ]
            self.run_s3cmd(client, put_args)

            # get object in cache
            get_file = '7M-get.dat'
            get_args = [
                'get',
                's3://{bucket}/{filename}'.format(bucket=bucket_name,
                                                  filename=put_file),
                '{path}/{filename}'.format(path=self.ctx.rgw_datacache.archive,
                                           filename=get_file),
                '--force',
            ]
            self.run_s3cmd(client, get_args)

            # get info of object
            object_stat_cmd = [
                'object',
                'stat',
                '--bucket={bucket}'.format(bucket=bucket_name),
                '--object={put_file}'.format(put_file=put_file),
            ]

            (_,object_stat_json) = rgwadmin(self.ctx, client, object_stat_cmd,
                                           check_status=True)
            object_name = object_stat_json['manifest']['prefix']

            # check if there are files in the cache directory to verify that the cache is enabled
            log.info("Check that datacache is enabled")
            datacache_fi = StringIO()
            self.ctx.cluster.only(client).run(
                args=[
                    'find',
                    '{path}'.format(path=self.ctx.rgw.datacache_path),
                    '-type', 'f',
                    run.Raw('|'),
                    'wc', '-l'
                ],
                stdout=datacache_fi,
            )
            cached_object_count = int(datacache_fi.getvalue())
            log.info("Number of files in datacache directory is %d", cached_object_count)
            if cached_object_count == 0:
                log.info("No files in the datacache directory, cache is disabled.")
                continue

            # get name of cached object and check if it exists in the cache
            datacache_fp = StringIO()
            self.ctx.cluster.only(client).run(
                args=[
                    'basename',
                    run.Raw('$('),
                    'find',
                    '{path}'.format(path=self.ctx.rgw.datacache_path),
                    '-name',
                    '*{file_name}*'.format(file_name=object_name),
                    run.Raw(')'),
                ],
                stdout=datacache_fp,
            )
            cached_object_filename = datacache_fp.getvalue()
            log.info("Name of file in datacache is %s", cached_object_filename)

            # check to see if the cached object is in Ceph
            rados_fp = StringIO()
            self.ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{path}/coverage'.format(path=self.ctx.rgw_datacache.archive),
                    'rados',
                    'ls',
                    '-p',
                    'default.rgw.buckets.data',
                ],
                stdout=rados_fp,
            )
            rados_output = rados_fp.getvalue()
            assert(cached_object_filename in rados_output)

        log.debug('RGW Datacache: TEST DONE')

    def end(self):
        super(RGWDataCache, self).begin()
        log.debug('RGW Datacache: CLEANUP BEGIN')
        for client in self.all_clients:
            self.ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{path}'.format(path=self.ctx.rgw.datacache_path),
                ],
                stdout=BytesIO(),
            )
        log.debug('RGW Datacache: CLEANUP DONE')

    def run_s3cmd(self, client, cmd_args):
        """
        Wrapper for running s3cmd
        """
        endpoint = self.ctx.rgw.role_endpoints.get(client)
        host = ""
        ssl = ""
        if endpoint.port == 443:
            host += "https://"
            ssl += "--ssl"
        else:
            host += "http://"
            ssl += "--no-ssl"
        host += '{ip}:{port}'.format(ip=endpoint.hostname, port=endpoint.port)
        s3cmd_args = [
            's3cmd',
            '--access_key={key}'.format(key=access_key),
            '--secret_key={key}'.format(key=secret_key),
            '--config={path}'.format(path=self.ctx.rgw_datacache.s3cmd_cfg),
            '{https}'.format(https=ssl),
            '--no-check-certificate',
            '--host={host}'.format(host=host),
        ] + cmd_args

        self.ctx.cluster.only(client).run(
            args=s3cmd_args,
            stdout=BytesIO(),
        )

    def create_s3cmd_config(self, client):
        """
        Creates a minimal config file for s3cmd
        """
        log.info("RGW Datacache: Creating s3cmd config...")
        (remote,) = self.ctx.cluster.only(client).remotes.keys()

        s3cmd_config = ConfigObj(
            indent_type='',
            infile={
                'default':
                    {
                    'host_bucket': 'no.way.in.hell',
                    'use_https': 'False',
                    },
                }
        )

        conf_fp = BytesIO()
        s3cmd_config.write(conf_fp)
        remote.write_file(self.ctx.rgw_datacache.s3cmd_cfg,
                          data=conf_fp.getvalue())
        log.info("RGW Datacache: s3cmd config at %s", self.ctx.rgw_datacache.s3cmd_cfg)

task = RGWDataCache
