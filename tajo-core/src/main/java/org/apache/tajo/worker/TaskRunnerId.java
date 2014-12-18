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

package org.apache.tajo.worker;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class TaskRunnerId extends ContainerId {
  ApplicationAttemptId applicationAttemptId;
  long id;

  public TaskRunnerId(ApplicationAttemptId applicationAttemptId, long id) {
    this.applicationAttemptId = applicationAttemptId;
    this.id = id;
  }

  public TaskRunnerId(YarnProtos.ContainerIdProto proto) {
    this.applicationAttemptId = new ApplicationAttemptIdPBImpl(proto.getAppAttemptId());
    this.id = proto.getId();
  }

  public TaskRunnerId(ContainerId containerId) {
    this(getContainerIdProto(containerId));
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  @Override
  public void setApplicationAttemptId(ApplicationAttemptId atId) {
    this.applicationAttemptId = atId;
  }

  @Override
  public int getId() {
    return (int) id;
  }

  @Override
  public long getContainerId() {
    return id;
  }

  @Override
  protected void setContainerId(long id) {
    this.id = id;
  }

  public YarnProtos.ContainerIdProto getProto() {
    YarnProtos.ApplicationIdProto appIdProto = YarnProtos.ApplicationIdProto.newBuilder()
        .setClusterTimestamp(applicationAttemptId.getApplicationId().getClusterTimestamp())
        .setId(applicationAttemptId.getApplicationId().getId())
        .build();

    YarnProtos.ApplicationAttemptIdProto attemptIdProto = YarnProtos.ApplicationAttemptIdProto.newBuilder()
        .setAttemptId(applicationAttemptId.getAttemptId())
        .setApplicationId(appIdProto)
        .build();

    return YarnProtos.ContainerIdProto.newBuilder()
        .setAppAttemptId(attemptIdProto)
        .setAppId(appIdProto)
        .setId(id)
        .build();
  }

  public static YarnProtos.ContainerIdProto getContainerIdProto(ContainerId containerId) {
    if(containerId instanceof TaskRunnerId) {
      return ((TaskRunnerId)containerId).getProto();
    } else {
      YarnProtos.ApplicationIdProto appIdProto = YarnProtos.ApplicationIdProto.newBuilder()
          .setClusterTimestamp(containerId.getApplicationAttemptId().getApplicationId().getClusterTimestamp())
          .setId(containerId.getApplicationAttemptId().getApplicationId().getId())
          .build();

      YarnProtos.ApplicationAttemptIdProto attemptIdProto = YarnProtos.ApplicationAttemptIdProto.newBuilder()
          .setAttemptId(containerId.getApplicationAttemptId().getAttemptId())
          .setApplicationId(appIdProto)
          .build();

      return YarnProtos.ContainerIdProto.newBuilder()
          .setAppAttemptId(attemptIdProto)
          .setAppId(appIdProto)
          .setId(containerId.getId())
          .build();
    }
  }

  public static TaskRunnerId getTaskRunnerIdByString(String containerId) {
    return new TaskRunnerId(ConverterUtils.toContainerId(containerId));
  }

  @Override
  protected void build() {

  }
}
