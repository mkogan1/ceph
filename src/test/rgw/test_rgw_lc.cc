// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_xml.h"
#include "rgw_lc.h"
#include "rgw_lc_s3.h"
#include <gtest/gtest.h>
//#include <spawn/spawn.hpp>
#include <string>
#include <vector>
#include <stdexcept>

static const char* xmldoc_1 =
R"(<Filter>
   <And>
      <Prefix>tax/</Prefix>
      <Tag>
         <Key>key1</Key>
         <Value>value1</Value>
      </Tag>
      <Tag>
         <Key>key2</Key>
         <Value>value2</Value>
      </Tag>
    </And>
</Filter>
)";

TEST(TestLCFilterDecoder, XMLDoc1)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_1, strlen(xmldoc_1), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check repeated Tag element */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("key1");
  ASSERT_EQ(val1->second, "value1");
  auto val2 = tag_map.find("key2");
  ASSERT_EQ(val2->second, "value2");
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), 0);
}

static const char* xmldoc_2 =
R"(<Filter>
   <And>
      <ArchiveZone />
      <Tag>
         <Key>spongebob</Key>
         <Value>squarepants</Value>
      </Tag>
    </And>
</Filter>
)";

TEST(TestLCFilterDecoder, XMLDoc2)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_2, strlen(xmldoc_2), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check tags */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("spongebob");
  ASSERT_EQ(val1->second, "squarepants");
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), LCFilter::make_flag(LCFlagType::ArchiveZone));
}

// invalid And element placement
static const char* xmldoc_3 =
R"(<Filter>
    <And>
      <Tag>
         <Key>miles</Key>
         <Value>davis</Value>
      </Tag>
    </And>
      <Tag>
         <Key>spongebob</Key>
         <Value>squarepants</Value>
      </Tag>
</Filter>
)";

TEST(TestLCFilterInvalidAnd, XMLDoc3)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_3, strlen(xmldoc_3), 1));
  LCFilter_S3 filter;
  auto result = RGWXMLDecoder::decode_xml("Filter", filter, &parser, true);
  ASSERT_TRUE(result);
  /* check repeated Tag element */
  auto tag_map = filter.get_tags().get_tags();
  auto val1 = tag_map.find("spongebob");
  ASSERT_TRUE(val1 == tag_map.end());
  /* because the invalid 2nd tag element was not recognized,
   * we cannot access it:
  ASSERT_EQ(val1->second, "squarepants");
  */
  /* check our flags */
  ASSERT_EQ(filter.get_flags(), uint32_t(LCFlagType::none));
}

static const char *xmldoc_4 =
R"(<Rule>
        <ID>noncur-cleanup-rule</ID>
        <Filter>
           <Prefix></Prefix>
        </Filter>
        <Status>Enabled</Status>
       <NoncurrentVersionExpiration>
            <NewerNoncurrentVersions>5</NewerNoncurrentVersions>
            <NoncurrentDays>365</NoncurrentDays>
       </NoncurrentVersionExpiration>
    </Rule>
)";

TEST(TestLCConfigurationDecoder, XMLDoc4)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_4, strlen(xmldoc_4), 1));
  LCRule_S3 rule;
  auto result = RGWXMLDecoder::decode_xml("Rule", rule, &parser, true);
  ASSERT_TRUE(result);
  /* check results */
  ASSERT_TRUE(rule.is_enabled());
  const auto& noncur_expiration = rule.get_noncur_expiration();
  ASSERT_EQ(noncur_expiration.get_days(), 365);
  ASSERT_EQ(noncur_expiration.get_newer(), 5);
}

static const char *xmldoc_5 =
R"(<Rule>
        <ID>expire-size-rule</ID>
        <Filter>
           <And>
              <Prefix></Prefix>
              <ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan>
              <ObjectSizeLessThan>65536</ObjectSizeGreaterThan>
           </And>
        </Filter>
        <Status>Enabled</Status>
       <Expiration>
            <Days>365</Days>
       </Expiration>
    </Rule>
)";

TEST(TestLCConfigurationDecoder, XMLDoc5)
{
  RGWXMLDecoder::XMLParser parser;
  ASSERT_TRUE(parser.init());
  ASSERT_TRUE(parser.parse(xmldoc_5, strlen(xmldoc_5), 1));
  LCRule_S3 rule;
  auto result = RGWXMLDecoder::decode_xml("Rule", rule, &parser, true);
  ASSERT_TRUE(result);
  /* check results */
  ASSERT_TRUE(rule.is_enabled());
  const auto& expiration = rule.get_expiration();
  ASSERT_EQ(expiration.get_days(), 365);
  const auto& filter = rule.get_filter();
  ASSERT_EQ(filter.get_size_gt(), 1024);
  ASSERT_EQ(filter.get_size_lt(), 65536);
}
