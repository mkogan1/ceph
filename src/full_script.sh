#!/bin/bash
redis-cli -h 127.0.0.1 -p 6379 FLUSHALL
../src/stop.sh
rm -rf cache_d3n/*
sleep 5
MON=1 OSD=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d -o debug_ms=0
sleep 5
echo $(cat out/radosgw.8000.pid)
./bin/radosgw-admin -c ceph.conf user create --uid=test4 --display-name=test4 --access-key=test4 --secret=test4 --system
sleep 2
fallocate -l 20M 20M.dat
sleep 5
s3cmd --access_key=test4 --secret_key=test4 --host=127.0.0.1:8000 mb s3://bkt
echo "---------------Created Bucket---------------"
sleep 5
s3cmd --access_key=test4 --secret_key=test4 --host=127.0.0.1:8000 put ./20M.dat s3://bkt --disable-multipart
echo "---------------Put Object Success---------------"
sleep 5
s3cmd --access_key=test4 --secret_key=test4 --host=127.0.0.1:8000 get s3://bkt/20M.dat get_obj_cache --force
echo "---------------Get Object Success---------------"
sleep 2
./bin/rados ls -p default.rgw.buckets.data > rgwKeys.txt
echo "---------------Saved to rgwKeys.txt---------------"
sleep 2
echo "keys *" | redis-cli -h 127.0.0.1 -p 6379 > metaKeys.txt
echo "---------------Saved to metaKeys.txt---------------"