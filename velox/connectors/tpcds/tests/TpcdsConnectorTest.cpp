/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/tpcds/TpcdsConnector.h"
#include <folly/init/Init.h>
#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::connector::tpcds;

using facebook::velox::exec::test::PlanBuilder;
using tpcds::Table;

class TpcdsConnectorTest : public exec::test::OperatorTestBase {
 public:
  const std::string kTpcdsConnectorId = "test-tpcds";

  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::registerConnectorFactory(
        std::make_shared<connector::tpcds::TpcdsConnectorFactory>());
    auto tpcdsConnector =
        connector::getConnectorFactory(
            connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName)
            ->newConnector(
                kTpcdsConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpcdsConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTpcdsConnectorId);
    connector::unregisterConnectorFactory(
        connector::tpcds::TpcdsConnectorFactory::kTpcdsConnectorName);
    OperatorTestBase::TearDown();
  }

  exec::Split makeTpcdsSplit(size_t totalParts = 1, size_t partNumber = 0)
      const {
    return exec::Split(std::make_shared<TpcdsConnectorSplit>(
        kTpcdsConnectorId, /*cacheable=*/true, totalParts, partNumber));
  }

  RowVectorPtr getResults(
      const core::PlanNodePtr& planNode,
      std::vector<exec::Split>&& splits) {
    return exec::test::AssertQueryBuilder(planNode)
        .splits(std::move(splits))
        .copyResults(pool());
  }
};

// Simple scan of first 5 rows of table 'item'.
TEST_F(TpcdsConnectorTest, simple) {
  auto plan = PlanBuilder()
                  .tpcdsTableScan(
                      Table::TBL_ITEM,
                      {"i_item_sk",
                       "i_item_id",
                       "i_rec_start_date",
                       "i_product_name"})
                  .limit(0, 1, false)
                  .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1}),
      makeFlatVector<StringView>({"AAAAAAAABAAAAAAA"}),
      makeFlatVector<int32_t>({4000}, DATE()),
      makeFlatVector<StringView>({"ought"}),
  });
  test::assertEqualVectors(expected, output);
}

// Extract single column from table 'item'.
TEST_F(TpcdsConnectorTest, singleColumn) {
  auto plan = PlanBuilder()
                  .tpcdsTableScan(Table::TBL_ITEM, {"i_product_name"})
                  .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ALGERIA",       "ARGENTINA", "BRAZIL", "CANADA",
      "EGYPT",         "ETHIOPIA",  "FRANCE", "GERMANY",
      "INDIA",         "INDONESIA", "IRAN",   "IRAQ",
      "JAPAN",         "JORDAN",    "KENYA",  "MOROCCO",
      "MOZAMBIQUE",    "PERU",      "CHINA",  "ROMANIA",
      "SAUDI ARABIA",  "VIETNAM",   "RUSSIA", "UNITED KINGDOM",
      "UNITED STATES",
  })});
  test::assertEqualVectors(expected, output);
  EXPECT_EQ("i_product_name", output->type()->asRow().nameOf(0));
}

// Check that aliases are correctly resolved.
TEST_F(TpcdsConnectorTest, singleColumnWithAlias) {
  const std::string aliasedName = "my_aliased_column_name";

  auto outputType = ROW({aliasedName}, {VARCHAR()});
  auto plan = PlanBuilder()
                  .startTableScan()
                  .outputType(outputType)
                  .tableHandle(std::make_shared<TpcdsTableHandle>(
                      kTpcdsConnectorId, Table::TBL_ITEM))
                  .assignments({
                      {aliasedName,
                       std::make_shared<TpcdsColumnHandle>("i_product_name")},
                      {"other_name",
                       std::make_shared<TpcdsColumnHandle>("i_product_name")},
                      {"third_column",
                       std::make_shared<TpcdsColumnHandle>("i_manager_id")},
                  })
                  .endTableScan()
                  .limit(0, 1, false)
                  .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto expected = makeRowVector({makeFlatVector<StringView>({
      "ought",
  })});
  test::assertEqualVectors(expected, output);

  EXPECT_EQ(aliasedName, output->type()->asRow().nameOf(0));
  EXPECT_EQ(1, output->childrenSize());
}

TEST_F(TpcdsConnectorTest, unknownColumn) {
  EXPECT_THROW(
      {
        PlanBuilder()
            .tpcdsTableScan(Table::TBL_ITEM, {"invalid_column"})
            .planNode();
      },
      VeloxUserError);
}

// Join item and inventory on item_sk.
TEST_F(TpcdsConnectorTest, join) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId itemScanId;
  core::PlanNodeId inventoryScanId;
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tpcdsTableScan(
              tpcds::Table::TBL_ITEM,
              {"i_item_sk", "i_product_name"},
              1.0 /*scaleFactor*/)
          .capturePlanNodeId(itemScanId)
          .hashJoin(
              {"i_item_sk"},
              {"inv_item_sk"},
              PlanBuilder(planNodeIdGenerator)
                  .tpcdsTableScan(
                      tpcds::Table::TBL_INVENTORY,
                      {"inv_item_sk"},
                      1.0 /*scaleFactor*/)
                  .capturePlanNodeId(inventoryScanId)
                  .planNode(),
              "", // extra filter
              {"i_product_name"})
          .singleAggregation({"i_product_name"}, {"count(1) as product_cnt"})
          .orderBy({"i_product_name"}, false)
          .planNode();

  auto output = exec::test::AssertQueryBuilder(plan)
                    .split(itemScanId, makeTpcdsSplit())
                    .split(inventoryScanId, makeTpcdsSplit())
                    .copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<StringView>(
          {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}),
      makeConstant<int64_t>(5, 5),
  });
  test::assertEqualVectors(expected, output);
}

TEST_F(TpcdsConnectorTest, inventoryDateCount) {
  auto plan = PlanBuilder()
                  .tpcdsTableScan(Table::TBL_INVENTORY, {"inv_date_sk"}, 0.01)
                  .filter("inv_date_sk = '1992-01-01'::DATE")
                  .limit(0, 10, false)
                  .planNode();

  auto output = getResults(plan, {makeTpcdsSplit()});
  auto inventoryDate = output->childAt(0)->asFlatVector<int32_t>();
  EXPECT_EQ("1992-01-01", DATE()->toString(inventoryDate->valueAt(0)));
  // Match with count obtained from Java.
  EXPECT_EQ(9, inventoryDate->size());
}

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
