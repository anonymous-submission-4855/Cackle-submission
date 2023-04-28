
file(GLOB_RECURSE starling_SRC ${PROJECT_SOURCE_DIR}/src/*.cpp)

list(REMOVE_ITEM AWSSDK_LINK_LIBRARIES
	curl)

foreach(querynum RANGE 1 22)

        list(REMOVE_ITEM starling_SRC
                ${PROJECT_SOURCE_DIR}/src/Query${querynum}Executor.cpp)

        list(APPEND starling_QUERY_SRC Query${querynum}Executor.cpp)

        add_executable(q${querynum}
                Query${querynum}Executor.cpp
                )

        add_custom_command(
                OUTPUT ${PROJECT_SOURCE_DIR}/src/Query${querynum}Executor.cpp
                OUTPUT ${PROJECT_SOURCE_DIR}/src/include/Query${querynum}Executor.h
                DEPENDS ${PROJECT_SOURCE_DIR}/gen_skeleton/q${querynum}.json
                DEPENDS ${PROJECT_SOURCE_DIR}/gen_skeleton/*.template
                DEPENDS ${PROJECT_SOURCE_DIR}/gen_skeleton/generate.py
                DEPENDS ${PROJECT_SOURCE_DIR}/gen_skeleton/tmp_params
                COMMAND python3 generate.py q${querynum}.json
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/gen_skeleton)

        target_link_libraries(q${querynum} starling_lib ${AWSSDK_LINK_LIBRARIES}
          ${Boost_LIBRARIES} evhtp ${LIBEVENT_LIBRARIES} lz4 ${PROTOBUF_LIBRARY} snappy
          z zstd orc mycachemessages)

        add_custom_target(zip-dir-q${querynum}
                COMMAND ${CMAKE_COMMAND} -E make_directory
                ${PROJECT_BINARY_DIR}/zip-build) 

        add_custom_target(create-zip-q${querynum}
               COMMAND cp $<TARGET_FILE:q${querynum}>
               ${PROJECT_BINARY_DIR}/zip-build/
               COMMAND cp
               ${PROJECT_SOURCE_DIR}/py/exec-caller.py
               ${PROJECT_BINARY_DIR}/zip-build/

               COMMAND zip lambda-zip.zip q${querynum} exec-caller.py
               COMMAND mv ${PROJECT_BINARY_DIR}/zip-build/lambda-zip.zip
               ${PROJECT_BINARY_DIR}
               DEPENDS q${querynum} zip-dir-q${querynum}
               WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/zip-build
               )

        add_custom_target(upload-q${querynum}
                COMMAND aws lambda update-function-code --function-name ${LAMBDA_FUNCTION_NAME} --zip-file fileb://${PROJECT_BINARY_DIR}/lambda-zip.zip
                COMMAND aws s3 cp 
                ${PROJECT_BINARY_DIR}/lambda-zip.zip s3://LAMBDA_ZIP_BUCKET/lambda-zips/q${querynum}.zip
                DEPENDS create-zip-q${querynum})
endforeach(querynum)


list(REMOVE_ITEM starling_SRC ${PROJECT_SOURCE_DIR}/src/AllQueries.cpp)

add_library(starling_lib ${starling_SRC})

add_executable(qall AllQueries.cpp ${starling_QUERY_SRC})
target_link_libraries(qall starling_lib ${AWSSDK_LINK_LIBRARIES}
  ${Boost_LIBRARIES} evhtp ${LIBEVENT_LIBRARIES} lz4 ${PROTOBUF_LIBRARIES} snappy
        z zstd orc mycachemessages)
add_custom_target(zip-dir-qall
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${PROJECT_BINARY_DIR}/zip-build) 

add_custom_target(create-zip-qall
       COMMAND cp $<TARGET_FILE:qall>
       ${PROJECT_BINARY_DIR}/zip-build/
       COMMAND cp
       ${PROJECT_SOURCE_DIR}/py/exec-caller.py
       ${PROJECT_BINARY_DIR}/zip-build/

       COMMAND zip lambda-zip.zip qall exec-caller.py
       COMMAND mv ${PROJECT_BINARY_DIR}/zip-build/lambda-zip.zip
       ${PROJECT_BINARY_DIR}
       DEPENDS qall zip-dir-qall
       WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/zip-build
       )

add_custom_target(upload
        COMMAND aws lambda update-function-code --function-name ${LAMBDA_FUNCTION_NAME} --zip-file fileb://${PROJECT_BINARY_DIR}/lambda-zip.zip
        COMMAND aws s3 cp 
        ${PROJECT_BINARY_DIR}/lambda-zip.zip s3://LAMBDA_ZIP_BUCKET/lambda-zips/qall.zip
        DEPENDS create-zip-qall)

add_custom_target(zip-dir-test
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${PROJECT_BINARY_DIR}/zip-build) 

add_custom_target(create-zip-test
       COMMAND cp $<TARGET_FILE:mytest>
       ${PROJECT_BINARY_DIR}/zip-build/
       COMMAND cp
       ${PROJECT_SOURCE_DIR}/py/exec-caller.py
       ${PROJECT_BINARY_DIR}/zip-build/

       COMMAND zip lambda-zip.zip mytest exec-caller.py
       COMMAND mv ${PROJECT_BINARY_DIR}/zip-build/lambda-zip.zip
       ${PROJECT_BINARY_DIR}
       DEPENDS mytest zip-dir-test
       WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/zip-build
       )

add_custom_target(upload-test
        COMMAND aws lambda update-function-code --function-name
        ${LAMBDA_FUNCTION_NAME}
        --zip-file fileb://${PROJECT_BINARY_DIR}/lambda-zip.zip
        DEPENDS create-zip-test)

