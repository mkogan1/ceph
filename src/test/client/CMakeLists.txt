if(${WITH_CEPHFS})
  add_executable(ceph_test_client
    main.cc
    alternate_name.cc
    ops.cc
    nonblocking.cc
    commands.cc
    zerocopy.cc
    fscrypt_conf.cc
    )
  target_link_libraries(ceph_test_client
    client
    global
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_client
    DESTINATION ${CMAKE_INSTALL_BINDIR})
  add_executable(ceph_test_zerocopy_sanity
          zerocopy_sanity.cc
  )
  add_ceph_unittest(ceph_test_zerocopy_sanity)
  target_link_libraries(ceph_test_zerocopy_sanity ceph-common cephfs global)

  add_executable(ceph_test_client_fscrypt
    main_fscrypt.cc
    alternate_name.cc
    ops.cc
    nonblocking.cc
    fscrypt_conf.cc
    )
  target_link_libraries(ceph_test_client_fscrypt
    client
    global
    ceph-common
    cephfs
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    )
  install(TARGETS ceph_test_client_fscrypt
    DESTINATION ${CMAKE_INSTALL_BINDIR})
endif(${WITH_CEPHFS})
