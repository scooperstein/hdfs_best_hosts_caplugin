I'm in the process of trying to remove some of the unnecessary files in the repository (I added them initially by accident
and now they won't go away!).

To test it you can do something like:

cd cmake/build
./src/classad_hdfs_scheduler_tester ./src/libclassad_hdfs_scheduler.so test_files/sample_classad.txt
