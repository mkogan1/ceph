function(build_qat)
  set(QAT_BINARY_DIR ${CMAKE_BINARY_DIR}/src/qatlib)
  set(QAT_INSTALL_DIR ${QAT_BINARY_DIR}/install)
  set(QAT_INCLUDE_DIR ${QAT_INSTALL_DIR}/include)
  set(QAT_LIBRARY_DIR ${QAT_INSTALL_DIR}/lib)
  set(QAT_LIBRARY ${QAT_LIBRARY_DIR}/libqat.a)
  set(QAT_USDM_LIBRARY ${QAT_LIBRARY_DIR}/libusdm.a)

  # this include directory won't exist until the install step, but the
  # imported targets need it early for INTERFACE_INCLUDE_DIRECTORIES
  file(MAKE_DIRECTORY "${QAT_INCLUDE_DIR}")

  set(configure_cmd env CC=${CMAKE_C_COMPILER} ./configure --prefix=${QAT_INSTALL_DIR})
  # disable systemd or 'make install' tries to write /usr/lib/systemd/system/qat.service
  list(APPEND configure_cmd --disable-systemd)
  # samples don't build on arm64
  list(APPEND configure_cmd --disable-samples)
  # build a static library with -fPIC that we can link into crypto/compressor plugins
  list(APPEND configure_cmd --with-pic --enable-static --disable-shared)

  # clear the DESTDIR environment variable from debian/rules
  set(NO_DESTDIR_COMMAND ${CMAKE_COMMAND} -E env --unset=DESTDIR)

  set(make_cmd ${NO_DESTDIR_COMMAND} make -j3)
  set(install_cmd ${NO_DESTDIR_COMMAND} make install)
  # 'make install' is missing one header that ceph requires, so copy it manually
  list(APPEND install_cmd COMMAND cmake -E copy <SOURCE_DIR>/quickassist/utilities/libusdm_drv/include/qae_mem_utils.h ${QAT_INCLUDE_DIR}/qat)

  include(ExternalProject)
  ExternalProject_Add(qatlib_ext
    SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/qatlib"
    CONFIGURE_COMMAND ./autogen.sh COMMAND ${configure_cmd}
    BUILD_COMMAND ${make_cmd}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS ${QAT_LIBRARY} ${QAT_USDM_LIBRARY}
    INSTALL_COMMAND ${install_cmd}
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_INSTALL ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # export vars for find_package(QAT)
  set(QAT_LIBRARY ${QAT_LIBRARY} PARENT_SCOPE)
  set(QAT_USDM_LIBRARY ${QAT_USDM_LIBRARY} PARENT_SCOPE)
  set(QAT_INCLUDE_DIR ${QAT_INCLUDE_DIR} PARENT_SCOPE)
  # library dir for BuildQATzip.cmake
  set(QAT_LIBRARY_DIR ${QAT_LIBRARY_DIR} PARENT_SCOPE)
endfunction()
