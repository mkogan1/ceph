# Tests for various assorted utility functions found within cephadm
#
from unittest import mock

import functools
import io
import os
import sys

import pytest

from tests.fixtures import (
    import_cephadm,
    mock_docker,
    mock_podman,
    with_cephadm_ctx,
)
from typing import Optional

from cephadmlib import context
from cephadmlib import systemd_unit
from cephadmlib.constants import CGROUPS_SPLIT_PODMAN_VERSION

_cephadm = import_cephadm()


def _get_unit_file(ctx, fsid, limit_core_infinity: Optional[bool] = False):
    return str(systemd_unit._get_unit_file(ctx, fsid, limit_core_infinity))


def test_docker_engine_requires_docker():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_docker()
    r = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Requires=docker.service' in r


def test_podman_engine_does_not_req_docker():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_podman()
    r = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Requires=docker.service' not in r


def test_podman_engine_forking_service():
    # verity that the podman service uses the forking service type
    # and related parameters
    ctx = context.CephadmContext()
    ctx.container_engine = mock_podman()
    r = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Type=forking' in r
    assert 'PIDFile=' in r
    assert 'ExecStartPre' in r
    assert 'ExecStopPost' in r


def test_podman_with_split_cgroups_sets_delegate():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_podman()
    ctx.container_engine.version = CGROUPS_SPLIT_PODMAN_VERSION
    r = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert 'Type=forking' in r
    assert 'Delegate=yes' in r


def _ignore_blank_lines(value):
    return [v for v in value.splitlines() if v]


def test_new_docker():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_docker()
    ru = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert _ignore_blank_lines(ru) == [
        '# generated by cephadm',
        '[Unit]',
        'Description=Ceph %i for 9b9d7609-f4d5-4aba-94c8-effa764d96c9',
        '# According to:',
        '#   http://www.freedesktop.org/wiki/Software/systemd/NetworkTarget',
        '# these can be removed once ceph-mon will dynamically change network',
        '# configuration.',
        'After=network-online.target local-fs.target time-sync.target docker.service',
        'Wants=network-online.target local-fs.target time-sync.target',
        'Requires=docker.service',
        'PartOf=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
        'Before=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
        '[Service]',
        'LimitNOFILE=1048576',
        'LimitNPROC=1048576',
        'EnvironmentFile=-/etc/environment',
        'ExecStart=/bin/bash '
        '/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.run',
        "ExecStop=-/bin/bash -c 'bash "
        "/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.stop'",
        'ExecStopPost=-/bin/bash '
        '/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.poststop',
        'KillMode=none',
        'Restart=on-failure',
        'RestartSec=10s',
        'TimeoutStartSec=200',
        'TimeoutStopSec=120',
        'StartLimitInterval=30min',
        'StartLimitBurst=5',
        '[Install]',
        'WantedBy=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
    ]


def test_new_podman():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_podman()
    ctx.container_engine.version = CGROUPS_SPLIT_PODMAN_VERSION
    ru = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
    assert _ignore_blank_lines(ru) == [
        '# generated by cephadm',
        '[Unit]',
        'Description=Ceph %i for 9b9d7609-f4d5-4aba-94c8-effa764d96c9',
        '# According to:',
        '#   http://www.freedesktop.org/wiki/Software/systemd/NetworkTarget',
        '# these can be removed once ceph-mon will dynamically change network',
        '# configuration.',
        'After=network-online.target local-fs.target time-sync.target',
        'Wants=network-online.target local-fs.target time-sync.target',
        'PartOf=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
        'Before=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
        '[Service]',
        'LimitNOFILE=1048576',
        'LimitNPROC=1048576',
        'EnvironmentFile=-/etc/environment',
        'ExecStart=/bin/bash '
        '/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.run',
        "ExecStop=-/bin/bash -c 'bash "
        "/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.stop'",
        'ExecStopPost=-/bin/bash '
        '/var/lib/ceph/9b9d7609-f4d5-4aba-94c8-effa764d96c9/%i/unit.poststop',
        'KillMode=none',
        'Restart=on-failure',
        'RestartSec=10s',
        'TimeoutStartSec=200',
        'TimeoutStopSec=120',
        'StartLimitInterval=30min',
        'StartLimitBurst=5',
        'ExecStartPre=-/bin/rm -f %t/%n-pid %t/%n-cid',
        'ExecStopPost=-/bin/rm -f %t/%n-pid %t/%n-cid',
        'Type=forking',
        'PIDFile=%t/%n-pid',
        'Delegate=yes',
        '[Install]',
        'WantedBy=ceph-9b9d7609-f4d5-4aba-94c8-effa764d96c9.target',
    ]


def test_limit_core_infinity():
    ctx = context.CephadmContext()
    ctx.container_engine = mock_podman()
    ctx.container_engine.version = CGROUPS_SPLIT_PODMAN_VERSION
    ru = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9', limit_core_infinity=False)
    assert 'LimitCORE=infinity' not in ru
    ru = _get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9', limit_core_infinity=True)
    assert 'LimitCORE=infinity' in ru
