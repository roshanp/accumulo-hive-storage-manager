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

package org.apache.accumulo.storagehandler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;



/**
 * Wraps RangeInputSplit into the older MapReduce split package.
 */
public class AccumuloSplit extends FileSplit implements InputSplit {
  private RangeInputSplit split;

    private static final Logger log = Logger.getLogger(AccumuloSplit.class);

  public AccumuloSplit() {
    super((Path) null, 0, 0, (String[]) null);
    split = new RangeInputSplit();
  }

  public AccumuloSplit(RangeInputSplit split, Path dummyPath) {
    super(dummyPath, 0, 0, (String[]) null);
    this.split = split;
  }

  public RangeInputSplit getSplit() {
    return this.split;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    split.readFields(in);
  }

  @Override
  public String toString() {
    return "TableSplit " + split;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    split.write(out);
  }

  @Override
  public long getLength() {
      int len = 0;
      try {
          return split.getLength();
      } catch (IOException e) {
          log.error("Error getting length for split: " + StringUtils.stringifyException(e));
      }
      return len;
  }

  @Override
  public String[] getLocations() throws IOException {
    return split.getLocations();
  }
}
