"""
Deploy and configure PyKMIP for Teuthology
"""
import argparse
import contextlib
import logging
import httplib
import tempfile
from urlparse import urlparse
import json
import os
from cStringIO import StringIO
from teuthology.orchestra.remote import Remote
import pprint

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.packaging import install_package
from teuthology.packaging import remove_package
from teuthology.exceptions import ConfigError
from util import get_remote_for_role

log = logging.getLogger(__name__)


def get_pykmip_dir(ctx):
    return '{tdir}/pykmip'.format(tdir=teuthology.get_testdir(ctx))

def run_in_pykmip_dir(ctx, client, args, **kwargs):
    (remote,) = [client] if isinstance(client,Remote) else ctx.cluster.only(client).remotes.keys()
    return remote.run(
        args=['cd', get_pykmip_dir(ctx), run.Raw('&&'), ] + args,
        **kwargs
    )

def run_in_pykmip_venv(ctx, client, args, **kwargs):
    return run_in_pykmip_dir(ctx, client,
        args = ['.', '.pykmipenv/bin/activate'.format(get_pykmip_dir(ctx)),
                         run.Raw('&&')
                        ] + args, **kwargs)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download PyKMIP from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading pykmip...')
    pykmipdir = get_pykmip_dir(ctx)

    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', 'master')
        repo = cconf.get('force-repo', 'https://github.com/OpenKMIP/PyKMIP')
        sha1 = cconf.get('sha1')
        log.info("Using branch '%s' for pykmip", branch)
        log.info('sha1=%s', sha1)

        ctx.cluster.only(client).run(
            args=[
                'git', 'clone', '-b', branch, repo,
                pykmipdir,
                ],
            )
        if sha1 is not None:
            run_in_pykmip_dir(ctx, client, [
                    'git', 'reset', '--hard', sha1,
                ],
            )
    try:
        yield
    finally:
        log.info('Removing pykmip...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', pykmipdir ],
            )

_bindep_txt = """# should be part of PyKMIP
libffi-dev [platform:dpkg]
libffi-devel [platform:rpm]
libssl-dev [platform:dpkg]
openssl-devel [platform:redhat]
libopenssl-devel [platform:suse]
libsqlite3-dev [platform:dpkg]
sqlite-devel [platform:rpm]
python-dev [platform:dpkg]
python-devel [(platform:redhat platform:base-py2)]
python3-dev [platform:dpkg]
python3-devel [(platform:redhat platform:base-py3) platform:suse]
python3 [platform:suse]
"""

@contextlib.contextmanager
def install_packages(ctx, config):
    """
    Download the packaged dependencies of PyKMIP.
    Remove install packages upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Installing system dependenies for PyKMIP...')

    packages = {}
    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        # use bindep to read which dependencies we need from temp/bindep.txt
        fd, local_temp_path = tempfile.mkstemp(suffix='.txt',
                                               prefix='bindep-')
        os.write(fd, _bindep_txt)
        os.close(fd)
        fd, remote_temp_path = tempfile.mkstemp(suffix='.txt',
                                               prefix='bindep-')
        os.close(fd)
        remote.put_file(local_temp_path, remote_temp_path)
        os.remove(local_temp_path)
        run_in_pykmip_venv(ctx, remote, ['pip', 'install', 'bindep'])
        r = run_in_pykmip_venv(ctx, remote,
                ['bindep', '--brief', '--file', remote_temp_path],
                stdout=StringIO(),
                check_status=False) # returns 1 on success?
        packages[client] = r.stdout.getvalue().splitlines()
        for dep in packages[client]:
            install_package(dep, remote)
    try:
        yield
    finally:
        log.info('Removing system dependencies of PyKMIP...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            for dep in packages[client]:
                remove_package(dep, remote)

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for PyKMIP using pip.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for pykmip...')
    for (client, _) in config.items():
        run_in_pykmip_dir(ctx, client, ['virtualenv', '.pykmipenv'])
        run_in_pykmip_venv(ctx, client, ['pip', 'install', 'pytz', '-e', get_pykmip_dir(ctx)])
    yield

def assign_ports(ctx, config, initial_port):
    """
    Assign port numbers starting from @initial_port
    """
    port = initial_port
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.items():
        for role in roles_for_host:
            if role in config:
                r = get_remote_for_role(ctx, role)
                role_endpoints[role] = r.ip_address, port, r.hostname
                port += 1

    return role_endpoints

def copy_policy_json(ctx, cclient, cconfig):
    run_in_pykmip_dir(ctx, cclient,
                        ['cp',
                         get_pykmip_dir(ctx)+'/examples/policy.json',
                         get_pykmip_dir(ctx)])

_pykmip_configuration = """# configuration for pykmip
[server]
hostname={ipaddr}
port={port}
certificate_path={servercert}
key_path={serverkey}
ca_path={clientca}
auth_suite=Basic
policy_path={confdir}
enable_tls_client_auth=True
tls_cipher_suites=
    TLS_RSA_WITH_AES_128_CBC_SHA256
    TLS_RSA_WITH_AES_256_CBC_SHA256
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384
logging_level=DEBUG
database_path={confdir}/pykmip.sqlite
"""

def create_pykmip_conf(ctx, cclient, cconfig):
    log.info('#0 cclient={} cconfig={}'.format(pprint.pformat(cclient),pprint.pformat(cconfig)))
    (remote,) = ctx.cluster.only(cclient).remotes.keys()
    pykmip_ipaddr, pykmip_port, pykmip_hostname = ctx.pykmip.endpoints[cclient]
    log.info('#1 ip,p,h {} {} {}'.format(pykmip_ipaddr, pykmip_port, pykmip_hostname))
    clientca = cconfig.get('clientca', None)
    log.info('#2 clientca {}'.format(clientca))
    serverkey = None
    servercert = cconfig.get('servercert', None)
    log.info('#3 servercert {}'.format(servercert))
    servercert = ctx.ssl_certificates.get(servercert)
    log.info('#4 servercert {}'.format(servercert))
    clientca = ctx.ssl_certificates.get(clientca)
    log.info('#5 clientca {}'.format(clientca))
    if servercert != None:
      serverkey = servercert.key
      servercert = servercert.certificate
      log.info('#6 serverkey {} servercert {}'.format(serverkey, servercert))
    if clientca != None:
      clientca = clientca.certificate
      log.info('#7 clientca {}'.format(clientca))
    if servercert == None or clientca == None or serverkey == None:
      log.info('#8 clientca {} serverkey {} servercert {}'.format(clientca, serverkey, servercert))
      raise ConfigError('pykmip: Missing/bad servercert or clientca')
    pykmipdir = get_pykmip_dir(ctx)
    kmip_conf = _pykmip_configuration.format(
        ipaddr=pykmip_ipaddr,
	port=pykmip_port,
	confdir=pykmipdir,
	hostname=pykmip_hostname,
	clientca=clientca,
	serverkey=serverkey,
	servercert=servercert
    )
    fd, local_temp_path = tempfile.mkstemp(suffix='.conf',
                                           prefix='pykmip')
    os.write(fd, kmip_conf)
    os.close(fd)
    remote.put_file(local_temp_path, pykmipdir+'/pykmip.conf')
    os.remove(local_temp_path)

@contextlib.contextmanager
def configure_pykmip(ctx, config):
    """
    Configure pykmip paste-api and pykmip-api.
    """
    assert isinstance(config, dict)
    (cclient, cconfig) = config.items()[0]

    copy_policy_json(ctx, cclient, cconfig)
    create_pykmip_conf(ctx, cclient, cconfig)
    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def run_pykmip(ctx, config):
    try:
        yield
    finally:
        return
    assert isinstance(config, dict)
    log.info('Running pykmip...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        cluster_name, _, client_id = teuthology.split_role(client)

        # start the public endpoint
        client_public_with_id = 'pykmip.public' + '.' + client_id

        run_cmd = ['cd', get_pykmip_dir(ctx), run.Raw('&&'),
                   '.', '.pykmipenv/bin/activate', run.Raw('&&'),
                   'HOME={}'.format(get_pykmip_dir(ctx)), run.Raw('&&'),
                   'bin/pykmip-api',
                   run.Raw('& { read; kill %1; }')]
                   #run.Raw('1>/dev/null')

        run_cmd = 'cd ' + get_pykmip_dir(ctx) + ' && ' + \
                  '. .pykmipenv/bin/activate && ' + \
                  'HOME={}'.format(get_pykmip_dir(ctx)) + ' && ' + \
                  'exec bin/pykmip-api & { read; kill %1; }'

        ctx.daemons.add_daemon(
            remote, 'pykmip', client_public_with_id,
            cluster=cluster_name,
            args=['bash', '-c', run_cmd],
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=get_pykmip_dir(ctx),
            wait=False,
            check_status=False,
        )

        # sleep driven synchronization
        run_in_pykmip_dir(ctx, client, ['sleep', '15'])
    try:
        yield
    finally:
        log.info('Stopping PyKMIP instance')
        ctx.daemons.get_daemon('pykmip', client_public_with_id,
                               cluster_name).stop()


@contextlib.contextmanager
def create_secrets(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    try:
        yield
    finally:
        return
    assert isinstance(config, dict)
    (cclient, cconfig) = config.items()[0]

    rgw_user = cconfig['rgw_user']

    keystone_role = cconfig.get('use-keystone-role', None)
    keystone_host, keystone_port = ctx.keystone.public_endpoints[keystone_role]
    pykmip_ipaddr, pykmip_port, pykmip_hostname = ctx.pykmip.endpoints[cclient]
    pykmip_url = 'http://{host}:{port}'.format(host=pykmip_hostname,
                                                 port=pykmip_port)
    log.info("pykmip_url=%s", pykmip_url)
    #fetching user_id of user that gets secrets for radosgw
    token_req = httplib.HTTPConnection(keystone_host, keystone_port, timeout=30)
    token_req.request(
        'POST',
        '/v2.0/tokens',
        headers={'Content-Type':'application/json'},
        body=json.dumps(
            {"auth":
             {"passwordCredentials":
              {"username": rgw_user["username"],
               "password": rgw_user["password"]
              },
              "tenantName": rgw_user["tenantName"]
             }
            }
        )
    )
    rgw_access_user_resp = token_req.getresponse()
    if not (rgw_access_user_resp.status >= 200 and
            rgw_access_user_resp.status < 300):
        raise Exception("Cannot authenticate user "+rgw_user["username"]+" for secret creation")
    #    baru_resp = json.loads(baru_req.data)
    rgw_access_user_data = json.loads(rgw_access_user_resp.read())
    rgw_user_id = rgw_access_user_data['access']['user']['id']

    if 'secrets' in cconfig:
        for secret in cconfig['secrets']:
            if 'name' not in secret:
                raise ConfigError('pykmip.secrets must have "name" field')
            if 'base64' not in secret:
                raise ConfigError('pykmip.secrets must have "base64" field')
            if 'tenantName' not in secret:
                raise ConfigError('pykmip.secrets must have "tenantName" field')
            if 'username' not in secret:
                raise ConfigError('pykmip.secrets must have "username" field')
            if 'password' not in secret:
                raise ConfigError('pykmip.secrets must have "password" field')

            token_req = httplib.HTTPConnection(keystone_host, keystone_port, timeout=30)
            token_req.request(
                'POST',
                '/v2.0/tokens',
                headers={'Content-Type':'application/json'},
                body=json.dumps(
                    {
                        "auth": {
                            "passwordCredentials": {
                                "username": secret["username"],
                                "password": secret["password"]
                            },
                            "tenantName":secret["tenantName"]
                        }
                    }
                )
            )
            token_resp = token_req.getresponse()
            if not (token_resp.status >= 200 and
                    token_resp.status < 300):
                raise Exception("Cannot authenticate user "+secret["username"]+" for secret creation")

            token_data = json.loads(token_resp.read())
            token_id = token_data['access']['token']['id']

            key1_json = json.dumps(
                {
                    "name": secret['name'],
                    "expiration": "2020-12-31T19:14:44.180394",
                    "algorithm": "aes",
                    "bit_length": 256,
                    "mode": "cbc",
                    "payload": secret['base64'],
                    "payload_content_type": "application/octet-stream",
                    "payload_content_encoding": "base64"
                })

            sec_req = httplib.HTTPConnection(pykmip_hostname, pykmip_port, timeout=30)
            try:
                sec_req.request(
                    'POST',
                    '/v1/secrets',
                    headers={'Content-Type': 'application/json',
                             'Accept': '*/*',
                             'X-Auth-Token': token_id},
                    body=key1_json
                )
            except:
                log.info("catched exception!")
                run_in_pykmip_dir(ctx, cclient, ['sleep', '900'])

            pykmip_sec_resp = sec_req.getresponse()
            if not (pykmip_sec_resp.status >= 200 and
                    pykmip_sec_resp.status < 300):
                raise Exception("Cannot create secret")
            pykmip_data = json.loads(pykmip_sec_resp.read())
            if 'secret_ref' not in pykmip_data:
                raise ValueError("Malformed secret creation response")
            secret_ref = pykmip_data["secret_ref"]
            log.info("secret_ref=%s", secret_ref)
            secret_url_parsed = urlparse(secret_ref)
            acl_json = json.dumps(
                {
                    "read": {
                        "users": [rgw_user_id],
                        "project-access": True
                    }
                })
            acl_req = httplib.HTTPConnection(secret_url_parsed.netloc, timeout=30)
            acl_req.request(
                'PUT',
                secret_url_parsed.path+'/acl',
                headers={'Content-Type': 'application/json',
                         'Accept': '*/*',
                         'X-Auth-Token': token_id},
                body=acl_json
            )
            pykmip_acl_resp = acl_req.getresponse()
            if not (pykmip_acl_resp.status >= 200 and
                    pykmip_acl_resp.status < 300):
                raise Exception("Cannot set ACL for secret")

            key = {'id': secret_ref.split('secrets/')[1], 'payload': secret['base64']}
            ctx.pykmip.keys[secret['name']] = key

    run_in_pykmip_dir(ctx, cclient, ['sleep', '3'])
    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure Keystone

    Example of configuration:

    tasks:
      - local_cluster:
          cluster_path: /home/adam/ceph-1/build
      - local_rgw:
      - tox: [ client.0 ]
      - pykmip:
          client.0:
            force-branch: master
            config:
              clientca: ca-ssl-cert
              servercert: pykmkp-ssl-cert-and-key
            secrets:
              - name: my-key-1
                base64: a2V5MS5GcWVxKzhzTGNLaGtzQkg5NGVpb1FKcFpGb2c=
              - name: my-key-2
                base64: a2V5Mi5yNUNNMGFzMVdIUVZxcCt5NGVmVGlQQ1k4YWg=
      - s3tests:
          client.0:
            force-branch: master
            kms_key: my-key-1
      - rgw:
          client.0:
            use-pykmip-role: client.0
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task keystone only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.keys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('pykmip', {}))

    log.debug('PyKMIP config is %s', config)

    if not hasattr(ctx, 'ssl_certificates'):
        raise ConfigError('pykmip must run after the openssl_keys task')


    ctx.pykmip = argparse.Namespace()
    ctx.pykmip.endpoints = assign_ports(ctx, config, 5696)
    ctx.pykmip.keys = {}
    
    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: install_packages(ctx=ctx, config=config),
        lambda: configure_pykmip(ctx=ctx, config=config),
        lambda: run_pykmip(ctx=ctx, config=config),
        lambda: create_secrets(ctx=ctx, config=config),
        ):
        yield
