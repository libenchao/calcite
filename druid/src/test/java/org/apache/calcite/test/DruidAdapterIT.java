/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.druid} package.
 *
 * <p>Druid must be up and running with foodmart and wikipedia datasets loaded. Follow the
 * instructions on <a href="https://github.com/zabetak/calcite-druid-dataset">calcite-druid-dataset
 * </a> to setup Druid before launching these tests.
 *
 * <p>Features not yet implemented:
 * <ul>
 *   <li>push LIMIT into "select" query</li>
 *   <li>push SORT and/or LIMIT into "groupBy" query</li>
 *   <li>push HAVING into "groupBy" query</li>
 * </ul>
 *
 * <p>These tests use TIMESTAMP WITH LOCAL TIME ZONE type for the
 * Druid timestamp column, instead of TIMESTAMP type as
 * {@link DruidAdapter2IT}.
 */
public class DruidAdapterIT {
  /** URL of the "druid-foodmart" model. */
  public static final URL FOODMART =
      DruidAdapterIT.class.getResource("/druid-foodmart-model.json");

  /** URL of the "druid-wiki" model
   * and the "wikipedia" data set. */
  public static final URL WIKI =
      DruidAdapterIT.class.getResource("/druid-wiki-model.json");

  /** URL of the "druid-wiki-no-columns" model
   * and the "wikipedia" data set. */
  public static final URL WIKI_AUTO =
      DruidAdapterIT.class.getResource("/druid-wiki-no-columns-model.json");

  /** URL of the "druid-wiki-no-tables" model
   * and the "wikipedia" data set. */
  public static final URL WIKI_AUTO2 =
      DruidAdapterIT.class.getResource("/druid-wiki-no-tables-model.json");

  private static final String VARCHAR_TYPE =
      "VARCHAR";

  private static final String FOODMART_TABLE = "\"foodmart\"";

  /** Whether to run this test. */
  private static boolean enabled() {
    return CalciteSystemProperty.TEST_DRUID.value();
  }

  @BeforeAll
  public static void assumeDruidTestsEnabled() {
    assumeTrue(enabled(), "Druid tests disabled. Add -Dcalcite.test.druid to enable it");
  }

  /** Creates a query against FOODMART with approximate parameters. */
  private CalciteAssert.AssertQuery foodmartApprox(String sql) {
    return approxQuery(FOODMART, sql);
  }

  /** Creates a query against WIKI with approximate parameters. */
  private CalciteAssert.AssertQuery wikiApprox(String sql) {
    return approxQuery(WIKI, sql);
  }

  private CalciteAssert.AssertQuery approxQuery(URL url, String sql) {
    return CalciteAssert.that()
        .enable(enabled())
        .withModel(url)
        .with(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT, true)
        .with(CalciteConnectionProperty.APPROXIMATE_TOP_N, true)
        .with(CalciteConnectionProperty.APPROXIMATE_DECIMAL, true)
        .query(sql);
  }

  /** Creates a fixture. */
  public static CalciteAssert.AssertThat fixture() {
    return CalciteAssert.that()
        .enable(enabled());
  }

  /** Creates a query against a data set given by a map. */
  private CalciteAssert.AssertQuery sql(String sql, URL url) {
    return fixture()
        .withModel(url)
        .query(sql);
  }

  /** Creates a query against the {@link #FOODMART} data set. */
  private CalciteAssert.AssertQuery sql(String sql) {
    return fixture()
        .withModel(FOODMART)
        .query(sql);
  }

  @Test void testInterleaveBetweenAggregateAndGroupOrderByOnMetrics() {
    final String sqlQuery = "select \"store_state\", \"brand_name\", \"A\" "
        + "from (\n"
        + "  select sum(\"store_sales\")-sum(\"store_cost\") as a, \"store_state\""
        + ", \"brand_name\"\n"
        + "  from \"foodmart\"\n"
        + "  group by \"store_state\", \"brand_name\" ) subq\n"
        + "order by \"A\" limit 5";
    String postAggString = "\"postAggregations\":[{\"type\":\"expression\",\"name\":\"A\","
        + "\"expression\":\"(\\\"$f2\\\" - \\\"$f3\\\")\"}";
    final String plan = "PLAN=EnumerableInterpreter\n"
        + "  DruidQuery(table=[[foodmart, foodmart]], intervals=[[1900-01-09T00:00:00.000Z/"
        + "2992-01-10T00:00:00.000Z]], projects=[[$63, $2, $90, $91]], groups=[{0, 1}], "
        + "aggs=[[SUM($2), SUM($3)]], post_projects=[[$0, $1, -($2, $3)]], sort0=[2], dir0=[ASC], "
        + "fetch=[5])";
    CalciteAssert.AssertQuery q = sql(sqlQuery, FOODMART)
        .explainContains(plan)
        .queryContains(new DruidChecker(postAggString));
    q.returnsOrdered("store_state=CA; brand_name=King; A=21.4632",
        "store_state=OR; brand_name=Symphony; A=32.176",
        "store_state=CA; brand_name=Toretti; A=32.2465",
        "store_state=WA; brand_name=King; A=34.6104",
        "store_state=OR; brand_name=Toretti; A=36.3");
  }
}
