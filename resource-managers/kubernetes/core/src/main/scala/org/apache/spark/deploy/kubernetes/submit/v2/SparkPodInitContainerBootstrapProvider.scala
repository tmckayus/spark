/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.submit.v2

import io.fabric8.kubernetes.api.model.ConfigMap

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.{SparkPodInitContainerBootstrap, SparkPodInitContainerBootstrapImpl}
import org.apache.spark.deploy.kubernetes.config._

private[spark] trait SparkPodInitContainerBootstrapProvider {
  def getInitContainerBootstrap(
      initContainerConfigMap: ConfigMap,
      initContainerConfigMapKey: String,
      submittedDependenciesPlugin: Option[SubmittedDependencyInitContainerVolumesPlugin])
      : SparkPodInitContainerBootstrap
}

private[spark] class SparkPodInitContainerBootstrapProviderImpl(sparkConf: SparkConf)
    extends SparkPodInitContainerBootstrapProvider {
  def getInitContainerBootstrap(
      initContainerConfigMap: ConfigMap,
      initContainerConfigMapKey: String,
      submittedDependenciesPlugin: Option[SubmittedDependencyInitContainerVolumesPlugin])
      : SparkPodInitContainerBootstrap = {
    val jarsDownloadPath = sparkConf.get(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION)
    val filesDownloadPath = sparkConf.get(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION)
    val mountTimeout = sparkConf.get(MOUNT_DEPENDENCIES_INIT_TIMEOUT)
    val initContainerImage = sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE)

    new SparkPodInitContainerBootstrapImpl(
      initContainerImage,
      jarsDownloadPath,
      filesDownloadPath,
      mountTimeout,
      initContainerConfigMap.getMetadata.getName,
      initContainerConfigMapKey,
      submittedDependenciesPlugin)
  }
}
