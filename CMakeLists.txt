cmake_minimum_required (VERSION 2.6)
project (hdfs_scheduler_caplugin)

set( CMAKE_MODULE_PATH
 ${PROJECT_SOURCE_DIR}/cmake )

find_package( Classad REQUIRED )

include_directories ("${PROJECT_SOURCE_DIR}" "/usr/java/jdk1.6.0_45/include/"
"/usr/java/jdk1.6.0_45/include/linux")
add_subdirectory(src)
