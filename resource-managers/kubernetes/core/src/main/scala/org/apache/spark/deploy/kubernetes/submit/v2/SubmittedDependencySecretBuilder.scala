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

import com.google.common.base.Charsets
import com.google.common.io.{BaseEncoding, Files}
import io.fabric8.kubernetes.api.model.{Secret, SecretBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

private[spark] trait SubmittedDependencySecretBuilder {
  /**
   * Construct a Kubernetes secret bundle that init-containers can use to retrieve an
   * application's dependencies.
   */
  def buildInitContainerSecret(
      kubernetesAppId: String, jarsSecret: String, filesSecret: String): Secret
}

private[spark] class SubmittedDependencySecretBuilderImpl(sparkConf: SparkConf)
    extends SubmittedDependencySecretBuilder {

  override def buildInitContainerSecret(
      kubernetesAppId: String, jarsSecret: String, filesSecret: String): Secret = {
    val stagingServiceSslOptions = new SecurityManager(sparkConf)
        .getSSLOptions(RESOURCE_STAGING_SERVER_SSL_NAMESPACE)
    val trustStoreBase64 = stagingServiceSslOptions.trustStore.map { trustStoreFile =>
      require(trustStoreFile.isFile, "Dependency server trustStore provided at" +
        trustStoreFile.getAbsolutePath + " does not exist or is not a file.")
      (INIT_CONTAINER_STAGING_SERVER_TRUSTSTORE_SECRET_KEY,
        BaseEncoding.base64().encode(Files.toByteArray(trustStoreFile)))
    }.toMap
    val jarsSecretBase64 = BaseEncoding.base64().encode(jarsSecret.getBytes(Charsets.UTF_8))
    val filesSecretBase64 = BaseEncoding.base64().encode(filesSecret.getBytes(Charsets.UTF_8))
    val secretData = Map(
      INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY -> jarsSecretBase64,
      INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY -> filesSecretBase64) ++
      trustStoreBase64
    val kubernetesSecret = new SecretBuilder()
      .withNewMetadata()
      .withName(s"$kubernetesAppId-spark-init")
      .endMetadata()
      .addToData(secretData.asJava)
      .build()
    kubernetesSecret
  }
}
