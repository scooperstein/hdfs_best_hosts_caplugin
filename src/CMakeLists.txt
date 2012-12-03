include_directories( ${CLASSAD_INCLUDES} )
add_library(classad_hdfs_scheduler MODULE hdfs_scheduler.cc)
target_link_libraries(classad_hdfs_scheduler ${CLASSAD_LIB}
/usr/lib64/libhdfs.so /usr/java/jdk1.6.0_37/jre/lib/amd64/server/libjvm.so )

add_executable(classad_hdfs_scheduler_tester test_main.cc)
target_link_libraries(classad_hdfs_scheduler_tester ${CLASSAD_LIB} )
