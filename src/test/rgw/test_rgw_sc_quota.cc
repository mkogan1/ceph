// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Unit tests for the per-storage-class quota feature.
 *
 * We exercise three layers in isolation:
 *
 *   1. The on-the-wire encoding of RGWQuotaInfo at v3 <-> v4 boundaries.
 *
 *   2. The composite-key construction (rgw_sc_quota_key).
 *
 *   3. The pure per-SC enforcement helpers (sc_enforcement_active,
 *      combine_sc_quota, sc_quota_would_exceed). The enforcement path
 *      itself (RGWQuotaHandlerImpl::check_sc_quota) needs a bucket index
 *      and is covered by the vstart / s3-tests runs.
 *
 * All cases are pure unit tests: no RADOS, no driver, no asio.
 */

#include <gtest/gtest.h>
#include <cerrno>
#include <limits>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/async/yield_context.h"

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "rgw_quota_types.h"
#include "rgw_sc_quota_types.h"
#include "rgw_sc_quota_checker.h"
#include "rgw_placement_types.h"
#include "rgw_basic_types.h"        // for rgw_bucket
#include "include/buffer.h"

using namespace rgw::quota;

namespace {
struct TestDpp : public DoutPrefixProvider {
  CephContext* get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
};
} // namespace


static rgw_placement_rule make_placement(const std::string& name,
                                         const std::string& sc) {
  return rgw_placement_rule(name, sc);
}

namespace {
// RAII installer for the global provider slot so tests don't leak state.
} // namespace

TEST(RGWQuotaInfoV4, RoundtripEmptyScMap) {
  RGWQuotaInfo in;
  in.enabled = true;
  in.max_size = 1024;
  in.max_objects = 10;

  ceph::buffer::list bl;
  encode(in, bl);

  RGWQuotaInfo out;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_TRUE(out.enabled);
  EXPECT_EQ(out.max_size, 1024);
  EXPECT_EQ(out.max_objects, 10);
  EXPECT_TRUE(out.storage_class_quotas.empty());
  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::LEGACY);
}

TEST(RGWQuotaInfoV4, RoundtripPopulatedScMap) {
  RGWQuotaInfo in;
  in.enabled = true;
  in.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  in.storage_class_quotas[rgw_sc_quota_key("default", "SSD")] =
    RGWStorageClassQuota{ 1024 * 1024, 100, true };
  in.storage_class_quotas[rgw_sc_quota_key("default", "GLACIER")] =
    RGWStorageClassQuota{ -1, 50, true };

  ceph::buffer::list bl;
  encode(in, bl);

  RGWQuotaInfo out;
  auto p = bl.cbegin();
  decode(out, p);

  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::HYBRID);
  ASSERT_EQ(out.storage_class_quotas.size(), 2u);

  const auto* ssd = out.get_sc_quota(rgw_sc_quota_key("default", "SSD"));
  ASSERT_NE(ssd, nullptr);
  EXPECT_EQ(ssd->max_size, 1024 * 1024);
  EXPECT_EQ(ssd->max_objects, 100);
  EXPECT_TRUE(ssd->enabled);

  const auto* gla = out.get_sc_quota(rgw_sc_quota_key("default", "GLACIER"));
  ASSERT_NE(gla, nullptr);
  EXPECT_EQ(gla->max_size, -1);
  EXPECT_EQ(gla->max_objects, 50);
}

/*
 * Critical backward-compat test: encode a v4 blob with NO sc quotas and
 * an explicit LEGACY mode, decode it, then re-encode it.  The behaviour
 * must be indistinguishable from how a pre-feature RGW would treat the
 * same RGWQuotaInfo -- empty map, legacy mode preserved.
 */
TEST(RGWQuotaInfoV4, RoundtripLegacyDefaultsStable) {
  for (auto& in : RGWQuotaInfo::generate_test_instances()) {
    ceph::buffer::list bl;
    encode(in, bl);
    RGWQuotaInfo out;
    auto p = bl.cbegin();
    decode(out, p);
    EXPECT_EQ(in.enabled, out.enabled);
    EXPECT_EQ(in.max_size, out.max_size);
    EXPECT_EQ(in.max_objects, out.max_objects);
    EXPECT_EQ(in.check_on_raw, out.check_on_raw);
    EXPECT_EQ(in.enforcement_mode, out.enforcement_mode);
    EXPECT_EQ(in.storage_class_quotas.size(), out.storage_class_quotas.size());
  }
}

TEST(RGWScQuotaKey, BasicComposition) {
  EXPECT_EQ(rgw_sc_quota_key("default-placement", "SSD"),
            "default-placement::SSD");
}

TEST(RGWScQuotaKey, EmptyStorageClassMapsToSTANDARD) {
  // get_canonical_storage_class("") -> "STANDARD"
  EXPECT_EQ(rgw_sc_quota_key("default-placement", ""),
            "default-placement::STANDARD");
}

TEST(RGWScQuotaKey, FromRule) {
  EXPECT_EQ(rgw_sc_quota_key(make_placement("p1", "GLACIER")),
            "p1::GLACIER");
}



// ---------------------------------------------------------------------------
// Per-SC enforcement helpers (rgw_sc_quota_checker.h)
// ---------------------------------------------------------------------------

static RGWStorageClassQuota make_sc_quota(int64_t max_size, int64_t max_objects,
                                          bool enabled = true) {
  RGWStorageClassQuota q;
  q.max_size = max_size;
  q.max_objects = max_objects;
  q.enabled = enabled;
  return q;
}

TEST(RGWScQuotaEnforcement, LegacyAndGlobalOnlyModesInactive) {
  RGWQuotaInfo q;
  q.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] = make_sc_quota(1, 1);
  q.enforcement_mode = RGWQuotaEnforcementMode::LEGACY;
  EXPECT_FALSE(sc_enforcement_active(q));
  q.enforcement_mode = RGWQuotaEnforcementMode::GLOBAL_ONLY;
  EXPECT_FALSE(sc_enforcement_active(q));
}

TEST(RGWScQuotaEnforcement, ScModesActiveOnlyWithEntries) {
  for (auto mode : {RGWQuotaEnforcementMode::STORAGE_CLASS,
                    RGWQuotaEnforcementMode::HYBRID}) {
    RGWQuotaInfo q;
    q.enforcement_mode = mode;
    EXPECT_FALSE(sc_enforcement_active(q));
    q.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] = make_sc_quota(100, 10);
    EXPECT_TRUE(sc_enforcement_active(q));
  }
}

TEST(RGWScQuotaEnforcement, DifferentScKeyNotMatched) {
  RGWQuotaInfo q;
  q.storage_class_quotas[rgw_sc_quota_key("p", "SSD")] = make_sc_quota(100, 10);
  EXPECT_NE(q.get_sc_quota(rgw_sc_quota_key("p", "SSD")), nullptr);
  EXPECT_EQ(q.get_sc_quota(rgw_sc_quota_key("p", "HDD")), nullptr);
  EXPECT_EQ(q.get_sc_quota(rgw_sc_quota_key("other", "SSD")), nullptr);
}

TEST(RGWScQuotaEnforcement, WouldExceedBoundaries) {
  EXPECT_FALSE(sc_quota_would_exceed(100, 60, 40));   // exactly at the limit
  EXPECT_TRUE (sc_quota_would_exceed(100, 60, 41));
  EXPECT_TRUE (sc_quota_would_exceed(100, 100, 1));
  EXPECT_FALSE(sc_quota_would_exceed(100, 0, 0));
  EXPECT_FALSE(sc_quota_would_exceed(-1, 1ULL << 40, 1ULL << 40));  // unlimited
}

TEST(RGWScQuotaEnforcement, WouldExceedOverflow) {
  EXPECT_TRUE(sc_quota_would_exceed(std::numeric_limits<int64_t>::max(),
                                    std::numeric_limits<uint64_t>::max() - 1, 10));
}

TEST(RGWScQuotaEnforcement, CombineNoneOrDisabled) {
  auto e = combine_sc_quota(nullptr, nullptr);
  EXPECT_FALSE(e.have_size_limit);
  EXPECT_FALSE(e.have_object_limit);

  auto disabled = make_sc_quota(10, 10, false);
  e = combine_sc_quota(&disabled, &disabled);
  EXPECT_FALSE(e.have_size_limit);
  EXPECT_FALSE(e.have_object_limit);
}

TEST(RGWScQuotaEnforcement, CombineSingleSide) {
  auto bq = make_sc_quota(100, -1);
  auto e = combine_sc_quota(&bq, nullptr);
  EXPECT_TRUE(e.have_size_limit);
  EXPECT_EQ(e.max_size, 100);
  EXPECT_FALSE(e.have_object_limit);

  auto uq = make_sc_quota(-1, 5);
  e = combine_sc_quota(nullptr, &uq);
  EXPECT_FALSE(e.have_size_limit);
  EXPECT_TRUE(e.have_object_limit);
  EXPECT_EQ(e.max_objects, 5);
}

TEST(RGWScQuotaEnforcement, CombineTighterWins) {
  auto bq = make_sc_quota(100, 3);
  auto uq = make_sc_quota(50, 10);
  auto e = combine_sc_quota(&bq, &uq);
  EXPECT_EQ(e.max_size, 50);
  EXPECT_EQ(e.max_objects, 3);
}

TEST(RGWScQuotaEnforcement, CombineIgnoresDisabledSide) {
  auto bq = make_sc_quota(100, 3);
  auto uq = make_sc_quota(1, 1, false);
  auto e = combine_sc_quota(&bq, &uq);
  EXPECT_EQ(e.max_size, 100);
  EXPECT_EQ(e.max_objects, 3);
}

TEST(RGWScQuotaInfoEncoding, RoundTripWithScQuota) {
  RGWQuotaInfo in;
  in.enabled = true;
  in.max_size = 1024;
  in.max_objects = 7;
  in.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  in.storage_class_quotas[rgw_sc_quota_key("p", "COLD")] = make_sc_quota(5242880, 3);

  ceph::buffer::list bl;
  encode(in, bl);
  RGWQuotaInfo out;
  auto it = bl.cbegin();
  decode(out, it);

  EXPECT_EQ(out.enabled, in.enabled);
  EXPECT_EQ(out.max_size, in.max_size);
  EXPECT_EQ(out.max_objects, in.max_objects);
  EXPECT_EQ(out.enforcement_mode, in.enforcement_mode);
  const auto* sc = out.get_sc_quota(rgw_sc_quota_key("p", "COLD"));
  ASSERT_NE(sc, nullptr);
  EXPECT_EQ(sc->max_size, 5242880);
  EXPECT_EQ(sc->max_objects, 3);
  EXPECT_TRUE(sc->enabled);
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(RGWQuotaInfoV4, JSONRoundtrip) {
  RGWQuotaInfo in;
  in.enabled          = true;
  in.max_size         = 1024 * 1024;
  in.enforcement_mode = RGWQuotaEnforcementMode::HYBRID;
  in.storage_class_quotas[rgw_sc_quota_key("default", "SSD")] =
      RGWStorageClassQuota{512 * 1024, 50, true};

  // Dump to JSON
  JSONFormatter f;
  f.open_object_section("quota");
  in.dump(&f);
  f.close_section();
  std::stringstream ss;
  f.flush(ss);

  // Parse back
  JSONParser p;
  ASSERT_TRUE(p.parse(ss.str().c_str(), ss.str().size()));
  RGWQuotaInfo out;
  out.decode_json(&p);

  EXPECT_EQ(out.enforcement_mode, RGWQuotaEnforcementMode::HYBRID);
  ASSERT_EQ(out.storage_class_quotas.size(), 1u);
  const auto* ssd = out.get_sc_quota(rgw_sc_quota_key("default", "SSD"));
  ASSERT_NE(ssd, nullptr);
  EXPECT_EQ(ssd->max_size, 512 * 1024);
  EXPECT_EQ(ssd->max_objects, 50);
  EXPECT_TRUE(ssd->enabled);
}
