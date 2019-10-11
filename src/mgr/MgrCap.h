// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGRCAP_H
#define CEPH_MGRCAP_H

#include <ostream>

#include "include/types.h"
#include "common/entity_name.h"

class CephContext;

static const __u8 MGR_CAP_R     = (1 << 1);      // read
static const __u8 MGR_CAP_W     = (1 << 2);      // write
static const __u8 MGR_CAP_X     = (1 << 3);      // execute
static const __u8 MGR_CAP_ANY   = 0xff;          // *

struct mgr_rwxa_t {
  __u8 val = 0U;

  mgr_rwxa_t() {}
  explicit mgr_rwxa_t(__u8 v) : val(v) {}

  mgr_rwxa_t& operator=(__u8 v) {
    val = v;
    return *this;
  }
  operator __u8() const {
    return val;
  }
};

std::ostream& operator<<(std::ostream& out, const mgr_rwxa_t& p);

struct MgrCapGrantConstraint {
  enum MatchType {
    MATCH_TYPE_NONE,
    MATCH_TYPE_EQUAL,
    MATCH_TYPE_PREFIX,
    MATCH_TYPE_REGEX
  };

  MatchType match_type = MATCH_TYPE_NONE;
  std::string value;

  MgrCapGrantConstraint() {}
  MgrCapGrantConstraint(MatchType match_type, std::string value)
    : match_type(match_type), value(value) {
  }
};

std::ostream& operator<<(std::ostream& out, const MgrCapGrantConstraint& c);

struct MgrCapGrant {
  /*
   * A grant can come in one of four forms:
   *
   *  - a blanket allow ('allow rw', 'allow *')
   *    - this will match against any service and the read/write/exec flags
   *      in the mgr code.  semantics of what X means are somewhat ad hoc.
   *
   *  - a service allow ('allow service mds rw')
   *    - this will match against a specific service and the r/w/x flags.
   *
   *  - a profile ('profile read-only')
   *    - this will match against specific MGR-enforced semantics of what
   *      this type of user should need to do.  examples include 'read-write',
   *      'read-only', 'crash'.
   *
   *  - a command ('allow command foo', 'allow command bar with arg1=val1 arg2 prefix val2')
   *      this includes the command name (the prefix string), and a set
   *      of key/value pairs that constrain use of that command.  if no pairs
   *      are specified, any arguments are allowed; if a pair is specified, that
   *      argument must be present and equal or match a prefix.
   */
  std::string service;
  std::string profile;
  std::string command;
  std::map<std::string, MgrCapGrantConstraint> command_args;

  // restrict by network
  std::string network;

  // these are filled in by parse_network(), called by MgrCap::parse()
  entity_addr_t network_parsed;
  unsigned network_prefix = 0;
  bool network_valid = true;

  void parse_network();

  mgr_rwxa_t allow;

  // explicit grants that a profile grant expands to; populated as
  // needed by expand_profile() (via is_match()) and cached here.
  mutable std::list<MgrCapGrant> profile_grants;

  void expand_profile() const;

  MgrCapGrant() : allow(0) {}
  MgrCapGrant(std::string&& service,
              std::string&& profile,
              std::string&& command,
              std::map<std::string, MgrCapGrantConstraint>&& command_args,
              mgr_rwxa_t allow)
    : service(std::move(service)), profile(std::move(profile)),
      command(std::move(command)), command_args(std::move(command_args)),
      allow(allow) {
  }

  /**
   * check if given request parameters match our constraints
   *
   * @param cct context
   * @param name entity name
   * @param service service (if any)
   * @param command command (if any)
   * @param command_args command args (if any)
   * @return bits we allow
   */
  mgr_rwxa_t get_allowed(
      CephContext *cct,
      EntityName name,
      const std::string& service,
      const std::string& command,
      const std::map<std::string, std::string>& command_args) const;

  bool is_allow_all() const {
    return (allow == MGR_CAP_ANY &&
            service.empty() &&
            profile.empty() &&
            command.empty());
  }
};

std::ostream& operator<<(std::ostream& out, const MgrCapGrant& g);

struct MgrCap {
  std::string text;
  std::vector<MgrCapGrant> grants;

  MgrCap() {}
  explicit MgrCap(const std::vector<MgrCapGrant> &g) : grants(g) {}

  std::string get_str() const {
    return text;
  }

  bool is_allow_all() const;
  void set_allow_all();
  bool parse(const std::string& str, std::ostream *err=NULL);

  /**
   * check if we are capable of something
   *
   * This method actually checks a description of a particular operation against
   * what the capability has specified.
   *
   * @param service service name
   * @param command command id
   * @param command_args
   * @param op_may_read whether the operation may need to read
   * @param op_may_write whether the operation may need to write
   * @param op_may_exec whether the operation may exec
   * @return true if the operation is allowed, false otherwise
   */
  bool is_capable(CephContext *cct,
		  EntityName name,
		  const std::string& service,
		  const std::string& command,
		  const std::map<std::string, std::string>& command_args,
		  bool op_may_read, bool op_may_write, bool op_may_exec,
		  const entity_addr_t& addr) const;

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MgrCap*>& ls);
};
WRITE_CLASS_ENCODER(MgrCap)

std::ostream& operator<<(std::ostream& out, const MgrCap& cap);

#endif // CEPH_MGRCAP_H
