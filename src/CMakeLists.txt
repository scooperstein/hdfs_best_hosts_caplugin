include_directories( ${CLASSAD_INCLUDES} )
add_library(classad_hdfs_best_hosts MODULE hdfs_best_hosts.cc)
target_link_libraries(classad_hdfs_best_hosts ${CLASSAD_LIB}
/usr/lib64/libhdfs.so /usr/java/jdk1.6.0_45/jre/lib/amd64/server/libjvm.so )

add_executable(classad_hdfs_best_hosts_tester test_main.cc)
target_link_libraries(classad_hdfs_best_hosts_tester ${CLASSAD_LIB} )
