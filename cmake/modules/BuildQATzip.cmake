function(build_qatzip)
  set(QATzip_REPO https://github.com/intel/qatzip.git)
  set(QATzip_TAG "v1.1.2")

  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  set(QATzip_SOURCE_DIR ${CMAKE_BINARY_DIR}/src/qatzip)
  set(QATzip_INSTALL_DIR ${QATzip_SOURCE_DIR}/install)
  set(QATzip_INCLUDE_DIR ${QATzip_INSTALL_DIR}/include)
  set(QATzip_LIBRARY ${QATzip_INSTALL_DIR}/lib/libqatzip.a)

  # this include directory won't exist until the install step, but the
  # imported targets need it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${QATzip_INCLUDE_DIR}")

  set(configure_cmd env CC=${CMAKE_C_COMPILER} ./configure --prefix=${QATzip_INSTALL_DIR})
  # build a static library with -fPIC that we can link into crypto/compressor plugins
  list(APPEND configure_cmd --with-pic --enable-static --disable-shared)
  if(QATDRV_INCLUDE_DIR)
    list(APPEND configure_cmd --with-ICP_ROOT=${QATDRV_INCLUDE_DIR})
  endif()
  if(QAT_INCLUDE_DIR)
    list(APPEND configure_cmd CFLAGS=-I${QAT_INCLUDE_DIR})
  endif()
  if(QAT_LIBRARY_DIR)
    list(APPEND configure_cmd LDFLAGS=-L${QAT_LIBRARY_DIR})
  endif()

  set(source_dir_args
    SOURCE_DIR ${QATzip_SOURCE_DIR}
    GIT_REPOSITORY ${QATzip_REPO}
    GIT_TAG ${QATzip_TAG}
    GIT_SHALLOW TRUE
    GIT_CONFIG advice.detachedHead=false)

  include(ExternalProject)
  ExternalProject_Add(qatzip_ext
    ${source_dir_args}
    CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
    BUILD_COMMAND ${make_cmd} -j3
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${QATzip_LIBRARY}
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)
  unset(make_cmd)

  # export vars for find_package(QATzip)
  set(QATzip_LIBRARIES ${QATzip_LIBRARY} PARENT_SCOPE)
  set(QATzip_INCLUDE_DIR ${QATzip_INCLUDE_DIR} PARENT_SCOPE)
  set(QATzip_INTERFACE_LINK_LIBRARIES QAT::qat QAT::usdm LZ4::LZ4 PARENT_SCOPE)
endfunction()
