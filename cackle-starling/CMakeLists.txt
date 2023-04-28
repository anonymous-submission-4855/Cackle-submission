cmake_minimum_required (VERSION 3.5)
project(Starling)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)

include(ProcessorCount)
ProcessorCount(SYSTEM_CORES)

add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)

set(MY_LIB_DIR ${PROJECT_BINARY_DIR}/libs)
set(AWS_SDK_SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/)
execute_process(COMMAND mkdir -p ${MY_LIB_DIR})
execute_process(COMMAND mkdir -p
        ${AWS_SDK_SOURCE_DIR}/build)

execute_process(COMMAND cmake -DBUILD_ONLY=s3
        -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${MY_LIB_DIR}
        -DCMAKE_INSTALL_LIBDIR=lib
	-DCURL_LIBRARY=/home/ec2-user/curl-7.83.1/build/lib/libcurl.a
        ${AWS_SDK_SOURCE_DIR}
        WORKING_DIRECTORY ${AWS_SDK_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} install
        WORKING_DIRECTORY ${AWS_SDK_SOURCE_DIR}/build)


set(ORC_SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/orc/)
execute_process(COMMAND mkdir -p
        ${ORC_SOURCE_DIR}/build)
execute_process(COMMAND cmake -DBUILD_JAVA=OFF
  -DBUILD_LIBHDFSPP=OFF -DBUILD_TOOLS=ON
        -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=${MY_LIB_DIR}
        -DCMAKE_INSTALL_LIBDIR=lib
        -DPROTOBUF_HOME=/usr/local/
        ${ORC_SOURCE_DIR}
        WORKING_DIRECTORY ${ORC_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} install
        WORKING_DIRECTORY ${ORC_SOURCE_DIR}/build)

set(LIBEVENT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/libevent/)
execute_process(COMMAND mkdir -p
        ${LIBEVENT_SOURCE_DIR}/build)
execute_process(COMMAND cmake 
        -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DEVENT__LIBRARY_TYPE=STATIC
        -DCMAKE_INSTALL_PREFIX=${MY_LIB_DIR}
        -DCMAKE_INSTALL_LIBDIR=lib
        ${LIBEVENT_SOURCE_DIR}
        WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} all 
        WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} install
        WORKING_DIRECTORY ${LIBEVENT_SOURCE_DIR}/build)

set(LIBEVHTP_SOURCE_DIR ${PROJECT_SOURCE_DIR}/third_party/libevhtp/)
execute_process(COMMAND mkdir -p
        ${LIBEVHTP_SOURCE_DIR}/build)
execute_process(COMMAND cmake 
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DEVHTP_DISABLE_SSL=ON
        -DEVHTP_DISABLE_EVTHR=ON
        -DEVHTP_BUILD_SHARED=OFF
        -DCMAKE_PREFIX_PATH=${MY_LIB_DIR}
        -DLIBEVENT_INCLUDE_DIR=${MY_LIB_DIR}/include
        -DCMAKE_INSTALL_PREFIX=${MY_LIB_DIR}
        ${LIBEVHTP_SOURCE_DIR}
        WORKING_DIRECTORY ${LIBEVHTP_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} all 
        WORKING_DIRECTORY ${LIBEVHTP_SOURCE_DIR}/build)
execute_process(COMMAND make -j${SYSTEM_CORES} install
        WORKING_DIRECTORY ${LIBEVHTP_SOURCE_DIR}/build)

execute_process(COMMAND ${PROJECT_SOURCE_DIR}/third_party/boost/bootstrap.sh
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/boost/)

execute_process(COMMAND ${PROJECT_SOURCE_DIR}/third_party/boost/b2
        --with-serialization --with-system --prefix=${MY_LIB_DIR} link=static
        variant=release threading=single -j ${SYSTEM_CORES} install
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/boost/)

set(Starling_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(Starling_THIRD_PARTY_INCLUDE_DIR ${PROJECT_BINARY_DIR}/libs/include)

link_directories(${PROJECT_BINARY_DIR}/libs/lib/)
set(CMAKE_PREFIX_PATH ${PROJECT_BINARY_DIR}/libs/lib/cmake/)
set(BOOST_ROOT ${PROJECT_BINARY_DIR}/libs/)

find_package(AWSSDK REQUIRED COMPONENTS s3)
find_package(Boost REQUIRED COMPONENTS system)
find_package(Libevent REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(ProtoFiles ${PROJECT_SOURCE_DIR}/proto/cackle_cache.proto)
#add_library(mycachemessages ${ProtoFiles})
add_library(mycachemessages ${ProtoFiles})
target_link_libraries(mycachemessages PUBLIC 
  ${Boost_LIBRARIES} protobuf::libprotobuf gRPC::grpc gRPC::grpc++)
target_include_directories(mycachemessages PUBLIC ${Starling_THIRD_PARTY_INCLUDE_DIR})
get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET mycachemessages LANGUAGE cpp PROTOC_OUT_DIR ${MY_LIB_DIR}/include)
protobuf_generate(TARGET mycachemessages LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}" PROTOC_OUT_DIR ${MY_LIB_DIR}/include) 

include_directories(${Starling_SRC_INCLUDE_DIR} ${Starling_THIRD_PARTY_INCLUDE_DIR} ${PROTOBUF_INCLUDE_DIR})
#set(CMAKE_CXX_COMPILER clang++)
#set(CMAKE_LINKER lld)
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libstdc++")

add_subdirectory(src)
