function(GitProject projectName)
    if(NOT projectName)
        message(FATAL_ERROR "GitProject requires a project name")
    endif()
    set(_options USES_TERMINAL_CONFIGURE)
    set(_oneValueArgs
        GIT_REPOSITORY
        GIT_TAG
        SOURCE_DIR
        BINARY_DIR
    )
    set(_multiValueArgs
        BUILD_COMMAND
    )
    cmake_parse_arguments(_git_project "${_options}" "${_oneValueArgs}" "${_multiValueArgs}" ${ARGN})
    if (NOT EXISTS "${_git_project_SOURCE_DIR}")
        execute_process(
            COMMAND git clone ${_git_project_GIT_REPOSITORY} ${_git_project_SOURCE_DIR} --branch ${_git_project_GIT_TAG} --recurse-submodules
        )
    endif()
    if (NOT EXISTS "${_git_project_BINARY_DIR}")
        list(JOIN _git_project_BUILD_COMMAND " " _COMMAND)
        execute_process(
            COMMAND sh -c ${_COMMAND}
            RESULT_VARIABLE RES 
            WORKING_DIRECTORY ${_git_project_SOURCE_DIR}
        )
        if (${RES})
            message(STATUS "BUILD FAILED, CLEAN UP")
            file(REMOVE_RECURSE ${_git_project_BINARY_DIR})
        endif()
    endif()
endfunction()