PROJECT(cif)
CMAKE_MINIMUM_REQUIRED (VERSION 2.6)
SET(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/common-index-format.proto)
ADD_LIBRARY(cif ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(cif
    protobuf::libprotobuf
)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

ENABLE_TESTING()
ADD_SUBDIRECTORY(test)

