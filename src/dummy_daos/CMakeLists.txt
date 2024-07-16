option(USE_PARALLAX "Use parallax.cc instead of daos.cc" OFF)

set (SOURCES 
    daos.h
    dummy_daos.h
)

if(USE_PARALLAX)
    list(APPEND SOURCES parallax.cc dummy_parallax.cc)
else()
    list(APPEND SOURCES daos.cc dummy_daos.cc)
endif()

ecbuild_add_library(

    TARGET dummy_daos

    INSTALL_HEADERS_LIST
        daos.h

    HEADER_DESTINATION ${INSTALL_INCLUDE_DIR}

    SOURCES
        ${SOURCES}

    PUBLIC_LIBS
        eckit
        uuid

)

if(HAVE_DAOS_ADMIN)

    ecbuild_add_library(

        TARGET dummy_daos_tests

        INSTALL_HEADERS LISTED

        HEADER_DESTINATION ${INSTALL_INCLUDE_DIR}

        SOURCES
            daos/tests_lib.h
            daos/tests_lib.cc
        
        PRIVATE_INCLUDES
            ${PROJECT_SOURCE_DIR}/../daos.h
            ${PROJECT_SOURCE_DIR}/../dummy_daos.h

        PRIVATE_LIBS
            dummy_daos

    )
    
endif()
