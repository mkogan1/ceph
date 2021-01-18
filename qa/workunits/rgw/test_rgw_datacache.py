#!/usr/bin/python3

import logging as log
from configobj import ConfigObj
import subprocess
import json
import os

"""
Runs a test against a rgw with the data cache enabled. A client must be
set in the config for this task. This client must be the same client
that is in the config for the `rgw` task.

In the `overrides` section `datacache` and `datacache` must be configured for 
the `rgw` task and the ceph conf overrides must contain the below config 
variables in the client section. 

`s3cmd` must be added as an extra_package to the install task.

In the `workunit` task, `- rgw/run-datacache.sh` must be set for the client that
is in the config for the `rgw` task. The `RGW_DATACACHE_PATH` variable must be
set in the workunit's `env` and it must match the `datacache_path` given to the
`rgw` task in `overrides`.
Ex:
- install:
    extra_packages:
      deb: ['s3cmd']
      rpm: ['s3cmd']
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
- workunit:
    clients:
      client.0:
      - rgw/run-datacache.sh
    env:
      RGW_DATACACHE_PATH: /tmp/rgw_datacache
    cleanup: true
"""

log.basicConfig(level=log.DEBUG)

""" Constants """
USER = 'rgw_datacache_user'
DISPLAY_NAME = 'DatacacheUser'
ACCESS_KEY = 'NX5QOQKC6BH2IDN8HC7A'
SECRET_KEY = 'LnEsqNNqZIpkzauboDcLXLcYaWwLQ3Kop0zAnKIn'
BUCKET_NAME = 'datacachebucket'
FILE_NAME = '7M.dat'
GET_FILE_NAME = '7M-get.dat'

def exec_cmd(cmd):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = proc.communicate()
        if proc.returncode == 0:
            log.info('command succeeded')
            if out is not None: log.info(out)
            return out
        else:
            raise Exception("error: %s \nreturncode: %s" % (err, proc.returncode))
    except Exception as e:
        log.error('command failed')
        log.error(e)
        return False

def get_radosgw_endpoint():
    out = exec_cmd('sudo netstat -nltp | grep radosgw')
    x = out.decode('utf8').split(" ")
    port = [i for i in x if ':' in i][0].split(':')[1]
    log.info('radosgw port: %s' % port)
    proto = "http"

    if port == '443':
        proto = "https"

    endpoint = proto + '://localhost:' + port
    return endpoint

def create_s3cmd_config(path):
    """
    Creates a minimal config file for s3cmd
    """
    log.info("RGW Datacache: Creating s3cmd config...")

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

    f = open(path, 'wb')
    s3cmd_config.write(f)
    f.close()

def get_cmd_output(cmd_out):
    out = cmd_out.decode('utf8')
    out = out.strip('\n')
    return out

def main():
    """
    execute the datacache test
    """
    # setup for test
    cache_dir = os.environ['RGW_DATACACHE_PATH']
    log.debug("datacache dir from config is: %s", cache_dir)

    out = exec_cmd('pwd')
    pwd = get_cmd_output(out)
    log.debug("pwd is: %s", pwd)

    endpoint = get_radosgw_endpoint()

    # create 7M file to put
    outfile = pwd + '/' + FILE_NAME
    exec_cmd('dd if=/dev/urandom of=%s bs=1M count=7' % (outfile))

    # create user
    exec_cmd('radosgw-admin user create --uid %s --display-name %s --access-key %s --secret %s' 
            % (USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY))

    # create s3cmd config
    s3cmd_config_path = pwd + '/s3cfg'
    create_s3cmd_config(s3cmd_config_path)

    # create a bucket
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --host=%s mb s3://%s' 
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, BUCKET_NAME))

    # put an object in the bucket
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --host=%s put %s s3://%s' 
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, outfile, BUCKET_NAME))

    # get object from bucket
    get_file_path = pwd + '/' + GET_FILE_NAME
    exec_cmd('s3cmd --access_key=%s --secret_key=%s --config=%s --host=%s get s3://%s/%s %s --force'
            % (ACCESS_KEY, SECRET_KEY, s3cmd_config_path, endpoint, BUCKET_NAME, FILE_NAME, get_file_path))

    # get info of object
    out = exec_cmd('radosgw-admin object stat --bucket=%s --object=%s' % (BUCKET_NAME, FILE_NAME))

    json_op = json.loads(out)
    cached_object_name = json_op['manifest']['prefix']
    log.debug("Cached object name is: %s", cached_object_name)

    # get name of cached object and check if it exists in the cache
    out = exec_cmd('basename $(find %s -name *%s*)' % (cache_dir, cached_object_name))
    basename_cmd_out = get_cmd_output(out)
    log.debug("Name of file in datacache is: %s", basename_cmd_out)

    # check to see if the cached object is in Ceph
    out = exec_cmd('rados ls -p default.rgw.buckets.data')
    rados_ls_out = get_cmd_output(out)
    log.debug("rados ls output is: %s", rados_ls_out)

    assert(basename_cmd_out in rados_ls_out)
    log.debug("RGW Datacache test SUCCESS")

    # remove datacache dir
    cmd = exec_cmd('rm -rf %s' % (cache_dir))
    log.debug("RGW Datacache dir deleted")


main()
log.info("Completed Datacache tests")
