cmake_minimum_required(VERSION 3.20)
project(IOCP_Engine VERSION 2.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(IOCP_ENGINE_DEFAULT_BUILD_SERVER ON)
if(NOT WIN32)
    set(IOCP_ENGINE_DEFAULT_BUILD_SERVER OFF)
endif()

option(IOCP_ENGINE_BUILD_TESTS "Build portable unit tests" ON)
option(IOCP_ENGINE_BUILD_SERVER
       "Build the Windows IOCP server"
       ${IOCP_ENGINE_DEFAULT_BUILD_SERVER})
option(IOCP_ENGINE_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" ON)

if(IOCP_ENGINE_BUILD_SERVER AND NOT WIN32)
    message(FATAL_ERROR
        "iocp_server uses Windows IOCP/Winsock APIs. "
        "Configure with -DIOCP_ENGINE_BUILD_SERVER=OFF on non-Windows hosts.")
endif()

include(FetchContent)
find_program(NODE_EXECUTABLE NAMES node node.exe REQUIRED)
set(FLATBUFFERS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_GRPCTEST OFF CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_SHAREDLIB OFF CACHE BOOL "" FORCE)
set(FLATBUFFERS_BUILD_FLATC ON CACHE BOOL "" FORCE)
FetchContent_Declare(
    flatbuffers
    GIT_REPOSITORY https://github.com/google/flatbuffers.git
    GIT_TAG v24.12.23
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(flatbuffers)

set(PROTOCOL_SCHEMA "${CMAKE_CURRENT_SOURCE_DIR}/schema/protocol.fbs")
set(CPP_GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/generated/cpp")
set(TS_GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/client/src/generated")
set(CPP_GENERATED_FILE "${CPP_GENERATED_DIR}/protocol_generated.h")
set(TS_GENERATED_FILES
    "${TS_GENERATED_DIR}/protocol.ts"
    "${TS_GENERATED_DIR}/realtime-engine.ts"
    "${TS_GENERATED_DIR}/realtime-engine/alert-severity.ts"
    "${TS_GENERATED_DIR}/realtime-engine/authenticate.ts"
    "${TS_GENERATED_DIR}/realtime-engine/authentication-result.ts"
    "${TS_GENERATED_DIR}/realtime-engine/booking-notification.ts"
    "${TS_GENERATED_DIR}/realtime-engine/court-status.ts"
    "${TS_GENERATED_DIR}/realtime-engine/domain-payload.ts"
    "${TS_GENERATED_DIR}/realtime-engine/equipment-status.ts"
    "${TS_GENERATED_DIR}/realtime-engine/equipment-update.ts"
    "${TS_GENERATED_DIR}/realtime-engine/error-code.ts"
    "${TS_GENERATED_DIR}/realtime-engine/error-response.ts"
    "${TS_GENERATED_DIR}/realtime-engine/facility-update.ts"
    "${TS_GENERATED_DIR}/realtime-engine/join-room.ts"
    "${TS_GENERATED_DIR}/realtime-engine/leave-room.ts"
    "${TS_GENERATED_DIR}/realtime-engine/match-info.ts"
    "${TS_GENERATED_DIR}/realtime-engine/match-score-update.ts"
    "${TS_GENERATED_DIR}/realtime-engine/match-status.ts"
    "${TS_GENERATED_DIR}/realtime-engine/network-message.ts"
    "${TS_GENERATED_DIR}/realtime-engine/payload.ts"
    "${TS_GENERATED_DIR}/realtime-engine/ping.ts"
    "${TS_GENERATED_DIR}/realtime-engine/pong.ts"
    "${TS_GENERATED_DIR}/realtime-engine/publish.ts"
    "${TS_GENERATED_DIR}/realtime-engine/resource-status.ts"
    "${TS_GENERATED_DIR}/realtime-engine/subscription-ack.ts"
    "${TS_GENERATED_DIR}/realtime-engine/subscription-operation.ts"
    "${TS_GENERATED_DIR}/realtime-engine/system-alert.ts"
)

add_custom_command(
    OUTPUT "${CPP_GENERATED_FILE}" ${TS_GENERATED_FILES}
    COMMAND "${CMAKE_COMMAND}" -E make_directory
            "${CPP_GENERATED_DIR}" "${TS_GENERATED_DIR}"
    COMMAND "$<TARGET_FILE:flatc>" --cpp --scoped-enums --gen-object-api
            -o "${CPP_GENERATED_DIR}" "${PROTOCOL_SCHEMA}"
    COMMAND "$<TARGET_FILE:flatc>" --ts
            -o "${TS_GENERATED_DIR}" "${PROTOCOL_SCHEMA}"
    COMMAND "${NODE_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/strip-generated-comments.mjs"
            "${CPP_GENERATED_DIR}" "${TS_GENERATED_DIR}"
    DEPENDS "${PROTOCOL_SCHEMA}" flatc
    VERBATIM
    COMMENT "Generating C++ and TypeScript protocol bindings"
)
add_custom_target(generate_schema
    DEPENDS "${CPP_GENERATED_FILE}" ${TS_GENERATED_FILES})

add_custom_target(verify_generated
    COMMAND "${CMAKE_COMMAND}"
            -DFLATC=$<TARGET_FILE:flatc>
            -DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
            -P "${CMAKE_CURRENT_SOURCE_DIR}/scripts/verify_generated.cmake"
    DEPENDS flatc
    VERBATIM
)

add_library(realtime_protocol STATIC
    src/transport/websocket_transport.cpp
    src/transport/websocket_transport.hpp
    src/core/memory_pool.hpp
    src/core/outbound_queue.hpp
    src/core/send_cursor.hpp
    src/core/session_handle.hpp
    src/protocol/framing.hpp
    src/pubsub/room_registry.hpp
)
target_include_directories(realtime_protocol PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_compile_features(realtime_protocol PUBLIC cxx_std_20)

function(enable_strict_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE
            /W4
            /permissive-
            /Zc:__cplusplus
            /EHsc
        )
        if(IOCP_ENGINE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
        if(IOCP_ENGINE_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()
enable_strict_warnings(realtime_protocol)

if(IOCP_ENGINE_BUILD_SERVER)
    add_executable(iocp_server
        src/main.cpp
        src/application/protocol_router.cpp
        src/application/protocol_router.hpp
        src/core/iocp_server.cpp
        src/core/iocp_server.hpp
        "${CPP_GENERATED_FILE}"
    )
    add_dependencies(iocp_server generate_schema)
    target_include_directories(iocp_server PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${CPP_GENERATED_DIR}"
        "${flatbuffers_SOURCE_DIR}/include"
    )
    target_link_libraries(iocp_server PRIVATE realtime_protocol flatbuffers)
    if(WIN32)
        target_compile_definitions(iocp_server PRIVATE
            NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0A00)
        target_link_libraries(iocp_server PRIVATE ws2_32 mswsock)
    endif()
    enable_strict_warnings(iocp_server)
endif()

if(IOCP_ENGINE_BUILD_TESTS)
    enable_testing()
    add_executable(protocol_tests tests/test_main.cpp)
    target_link_libraries(protocol_tests PRIVATE realtime_protocol)
    enable_strict_warnings(protocol_tests)
    add_test(NAME protocol_tests COMMAND protocol_tests)
endif()
