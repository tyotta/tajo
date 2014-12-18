/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.util.*;

public class TpchTestBase {
  private static final Log LOG = LogFactory.getLog(TpchTestBase.class);

  String [] names;
  String [] paths;
  String [][] tables;
  Schema[] schemas;
  Map<String, Integer> nameMap = Maps.newHashMap();
  protected TPCH tpch;
  protected LocalTajoTestingUtility util;

  private static TpchTestBase testBase;

  static {
    try {
      testBase = new TpchTestBase();
      testBase.setUp();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private TpchTestBase() throws IOException {
    names = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier", "empty_orders"};
    paths = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      nameMap.put(names[i], i);
    }

    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();

    schemas = new Schema[names.length];
    for (int i = 0; i < names.length; i++) {
      schemas[i] = tpch.getSchema(names[i]);
    }

    tables = new String[names.length][];
    File file;
    for (int i = 0; i < names.length; i++) {
      file = new File("src/test/tpch/" + names[i] + ".tbl");
      if(!file.exists()) {
        file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + names[i]
            + ".tbl");
      }
      tables[i] = FileUtil.readTextFile(file).split("\n");
      paths[i] = file.getAbsolutePath();
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void setUp() throws Exception {
    util = new LocalTajoTestingUtility();
    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    util.setup(names, paths, schemas, opt);
  }

  public static TpchTestBase getInstance() {
    return testBase;
  }

  public ResultSet execute(String query) throws Exception {
    return util.execute(query);
  }

  public TajoTestingCluster getTestingCluster() {
    return util.getTestingCluster();
  }

  public void tearDown() throws IOException {
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
    }
    util.shutdown();
  }

  public static void main(String[] args) throws Exception {
//    2G,  291,  1,  68,  0.06076281,  1.9411764,  0.18860412286777123,  1.0,  14,  31.43,  5.29,  3.22,  1.0
//    3G,  290,  1,  6967,  0.43987042,  111.90929,  19.892839910073643,  9.43419,  25,  0.01,  11.72,  0.04,  1.04
//    3G,  588,  1,  3460,  3.365571,  55.407227,  4.830775134136222,  1.9734104,  3,  0.51,  11.33,  0.06,  1.0
//    4G,  53,  1,  2700545,  6.1362023,  36.1312,  4.191398728466538,  3.0741665,  1157961,  4.48,  88.82,  2.56,  2.45
//    4G,  587,  1,  556420,  1.977377,  34.175583,  4.207105154379785,  5.1599026,  365554,  0.74,  94.24,  2.79,  4.09
//    4G,  730,  1,  4423,  2.7834828,  31.77436,  1.3690284016605097,  1.0633055,  2042,  1.93,  24.01,  0.83,  1.08
//    3G,  676,  3,  106,  0.93941295,  20.679245,  1.870117138691668,  1.5566038,  ,  ,  ,  ,
//    4G,  161,  3,  32,  7.997549,  62.875,  1.793029048666358,  1.40625,  21,  27.72,  67.38,  1.91,  1.24
//    4G,  459,  3,  418,  0.57909596,  15.023924,  0.9739821776533812,  1.3229665,  279,  0.55,  56.88,  1.17,  1.23

    String[][] data = new String[][]{
        {"2G", "291", "1"},
        {"3G", "290", "1"},
        {"3G", "588", "1"},
        {"4G", "53", "1"},
        {"4G", "587", "1"},
        {"4G", "730", "1"},
        {"3G", "676", "3"},
        {"4G", "161", "3"},
        };

    Random rand = new Random();
    data = new String[1000][];
    String[] networks = new String[]{"2G", "3G", "4G"};
    String[] sex = new String[]{"1", "2"};

    for (int i = 0; i < 1000; i++) {
      data[i] = new String[]{ networks[rand.nextInt(3)], String.valueOf(rand.nextInt(500) + 1), sex[rand.nextInt(2)]};
    }

    int numPartitions = 62;
    Set<Integer> ids = new TreeSet<Integer>();
    for (int i = 0; i < data.length; i++) {
      VTuple vtuple = new VTuple(new Datum[]{new TextDatum(data[i][0]), new TextDatum(data[i][1]), new TextDatum(data[i][2])});

      String aaa = data[i][0] + data[i][1] + data[i][2];
      int hashValue = vtuple.hashCode();
//      int hashValue = MurmurHash.hash(aaa.getBytes(), 62);

      int hashValue2 = (hashValue & Integer.MAX_VALUE) % (numPartitions == 32 ? numPartitions-1 : numPartitions);

      ids.add(hashValue2);
      System.out.println(hashValue + "," + hashValue2 + "," + vtuple);
    }

    System.out.println("==================:" + ids.size());
    for (Integer id: ids) {
      System.out.println(id);
    }
  }
}
