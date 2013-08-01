This plugin is based on a framework set up by Brian Bockelman

To set up:

```shell
source old_stuff/setenv.sh
cd cmake/build
cmake ../..
make
```

Then to test:

```shell
./src/classad_hdfs_best_hosts_tester ./src/libclassad_hdfs_best_hosts.so test_files/sample_classad.txt
```
