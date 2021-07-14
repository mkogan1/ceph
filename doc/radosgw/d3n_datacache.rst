====================
D3N RGW Data caching
====================

.. contents::

Datacenter-Data-Delivery Network (D3N) uses high-speed storage such as NVMe flash or DRAM to cache
datasets on the access side.
Such caching allows big data jobs to use the compute and fast storage resources available on each
Rados Gateway node at the edge.

Many datacenters include low-cost, centralized storage repositories, called data lakes,
to store and share terabyte and petabyte-scale datasets.
By necessity most distributed big-data analytic clusters such as Hadoop and Spark must
depend on accessing a centrally located data lake that is relatively far away.
Even with a well-designed datacenter network, cluster-to-data lake bandwidth is typically much less
than the bandwidth of a solid-state storage located at an edge node.

| D3N improves the performance of big-data jobs running in analysis clusters by speeding up recurring reads from the data lake.
| The Rados Gateways act as cache servers for the back-end object store (OSDs), storing data locally for reuse.

Architecture
============

D3N improves the performance of big-data jobs by speeding up repeatedly accessed dataset reads from the data lake.
Cache servers are located in the datacenter on the access side of potential network and storage bottlenecks,
D3Ns two layer logical caches forms a traditional caching hierarchy :sup:`*`
where caches nearer the client have the lowest access latency and overhead,
while caches in higher levels in the hierarchy are slower (requiring multiple hops to access),
The layer 1 cache server nearest to the client handles object requests by breaking them into blocks,
returning any blocks which are cached locally, and forwarding missed requests to the block home location
(as determined by consistent hashing) in the next layer.
Cache misses are forwarded to successive logical caching layers until a miss at the top layer is resolved
by a request to the data lake (Rados)

:sup:`*` currently only layer 1 cache has been upstreamed.

See `MOC D3N (Datacenter-scale Data Delivery Network)`_ and `Red Hat Research D3N Cache for Data Centers`_.

Implementation
==============

- D3N cache supports both the `S3` and `Swift` object storage interfaces.
- D3N will cache tail objects because they are immutable (by default it is parts of objects that are larger than 4MB).

Requirements
------------

- A SSD(/dev/nvme,/dev/pmem,/dev/shm) storage device, formatted (filesystems other than XFS were not tested) and mounted,
  it will be used as the cache backing store.
  (depending on device performance, multiple RGWs may share a single device but each requires a discrete directory on the device)

Limitations
-----------

- D3N will not cache objects compressed by `Rados Gateway Compression`_ (OSD level compression is supported).
- D3N will not cache objects encrypted by `Rados Gateway Encryption`_.
- D3N will be disabled if the `rgw_max_chunk_size` config variable value differs from the `rgw_obj_stripe_size` config variable value.


D3N Environment Setup
=====================

Running
-------

D3N minimal configuration startup example using `vstart.sh`::

    env MON=1 OSD=1 MDS=0 MGR=1 RGW=1 ../src/vstart.sh -n  \
    -o rgw_d3n_l1_local_datacache_enabled=true  \
    -o rgw_d3n_l1_datacache_persistent_path=/mnt/nvme0/rgw_datacache/  \
    -o rgw_d3n_l1_datacache_size=10737418240

the above example assumes that the cache backing-store solid state device
is mounted at `/mnt/nvme0` and has `10 GB` of free space available.

    NOTE: Each time the Rados Gateway is restarted the content of the cache directory is purged.

Logs
----
- D3N related log lines in `radosgw.*.log` contain the string `d3n` (case insensitive).
- low level D3N logs can be enabled by the `debug_rgw_datacache` subsystem (up to `debug_rgw_datacache=30`)


Configuration
=============

D3N related `ceph.conf` configuration variables:

+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+
| Configuration Variable                             | Type     | Description                                                                                    |
+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+
| ``rgw_d3n_l1_local_datacache_enabled``             | bool     | Enable datacenter-scale dataset delivery local cache                                           |
+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+
| ``rgw_d3n_l1_datacache_persistent_path``           | string   | path of the directory for storing the local cache objects data                                 |
+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+
| ``rgw_d3n_l1_datacache_size``                      | size     | datacache maximum size on disk in bytes                                                        |
+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+
| ``rgw_d3n_l1_eviction_policy``                     | enum     | select the d3n cache eviction policy, allowed values: (`lru`|`random`)                         |
+----------------------------------------------------+----------+------------------------------------------------------------------------------------------------+



.. _MOC D3N (Datacenter-scale Data Delivery Network): https://massopen.cloud/research-and-development/cloud-research/d3n/
.. _Red Hat Research D3N Cache for Data Centers: https://research.redhat.com/blog/research_project/d3n-multilayer-cache/
.. _Rados Gateway Compression: https://docs.ceph.com/en/latest/radosgw/compression/
.. _Rados Gateway Encryption: https://docs.ceph.com/en/latest/radosgw/encryption/
