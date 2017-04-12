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
package org.apache.spark.deploy.rest.kubernetes.v2

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.security.SecureRandom
import java.util.UUID
import javax.ws.rs.core.StreamingOutput

import com.google.common.io.{BaseEncoding, ByteStreams, Files}
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.deploy.rest.kubernetes.v1.KubernetesCredentials
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] class ResourceStagingServiceImpl(dependenciesRootDir: File)
    extends ResourceStagingService with Logging {

  private val DIRECTORIES_LOCK = new Object
  private val SPARK_APPLICATION_DEPENDENCIES_LOCK = new Object
  private val SECURE_RANDOM = new SecureRandom()
  // TODO clean up these resources based on the driver's lifecycle
  private val sparkApplicationDependencies = mutable.Map.empty[String, SparkApplicationDependencies]

  override def uploadResources(
      podLabels: Map[String, String],
      podNamespace: String,
      resources: InputStream,
      kubernetesCredentials: KubernetesCredentials): String = {
    val resourcesId = UUID.randomUUID().toString
    val secretBytes = new Array[Byte](1024)
    SECURE_RANDOM.nextBytes(secretBytes)
    val applicationSecret = resourcesId + "-" + BaseEncoding.base64().encode(secretBytes)

    val namespaceDir = new File(dependenciesRootDir, podNamespace)
    val resourcesDir = new File(namespaceDir, resourcesId)
    try {
      DIRECTORIES_LOCK.synchronized {
        if (!resourcesDir.exists()) {
          if (!resourcesDir.mkdirs()) {
            throw new SparkException("Failed to create dependencies directory for application" +
              s" at ${resourcesDir.getAbsolutePath}")
          }
        }
      }
      // TODO encrypt the written data with the secret.
      val resourcesTgz = new File(resourcesDir, "resources.tgz")
      Utils.tryWithResource(new FileOutputStream(resourcesTgz)) { ByteStreams.copy(resources, _) }
      SPARK_APPLICATION_DEPENDENCIES_LOCK.synchronized {
        sparkApplicationDependencies(applicationSecret) = SparkApplicationDependencies(
          podLabels,
          podNamespace,
          resourcesTgz,
          kubernetesCredentials)
      }
      applicationSecret
    } catch {
      case e: Throwable =>
        if (!resourcesDir.delete()) {
          logWarning(s"Failed to delete application directory $resourcesDir.")
        }
        throw e
    }
  }

  override def downloadResources(applicationSecret: String): StreamingOutput = {
    val applicationDependencies = SPARK_APPLICATION_DEPENDENCIES_LOCK.synchronized {
      sparkApplicationDependencies
        .get(applicationSecret)
        .getOrElse(throw new SparkException("No application found for the provided token."))
    }
    new StreamingOutput {
      override def write(outputStream: OutputStream) = {
        Files.copy(applicationDependencies.resourcesTgz, outputStream)
      }
    }
  }
}

private case class SparkApplicationDependencies(
  podLabels: Map[String, String],
  podNamespace: String,
  resourcesTgz: File,
  kubernetesCredentials: KubernetesCredentials)
