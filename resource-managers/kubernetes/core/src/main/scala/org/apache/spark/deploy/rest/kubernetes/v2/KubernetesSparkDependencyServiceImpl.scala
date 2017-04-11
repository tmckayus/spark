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
import org.apache.spark.deploy.rest.KubernetesCredentials
import org.apache.spark.util.Utils

private[spark] class KubernetesSparkDependencyServiceImpl(dependenciesRootDir: File)
    extends KubernetesSparkDependencyService {

  private val DIRECTORIES_LOCK = new Object
  private val REGISTERED_DRIVERS_LOCK = new Object
  private val SPARK_APPLICATION_DEPENDENCIES_LOCK = new Object
  private val SECURE_RANDOM = new SecureRandom()
  // TODO clean up these resources based on the driver's lifecycle
  private val registeredDrivers = mutable.Set.empty[PodNameAndNamespace]
  private val sparkApplicationDependencies = mutable.Map.empty[String, SparkApplicationDependencies]

  override def uploadDependencies(
      driverPodName: String,
      driverPodNamespace: String,
      jars: InputStream,
      files: InputStream,
      kubernetesCredentials: KubernetesCredentials): String = {
    val podNameAndNamespace = PodNameAndNamespace(
      name = driverPodName,
      namespace = driverPodNamespace)
    REGISTERED_DRIVERS_LOCK.synchronized {
      if (registeredDrivers.contains(podNameAndNamespace)) {
        throw new SparkException(s"Spark application with driver pod named $driverPodName" +
          s" and namespace $driverPodNamespace already uploaded its dependencies.")
      }
      registeredDrivers.add(podNameAndNamespace)
    }
    try {
      val secretBytes = new Array[Byte](1024)
      SECURE_RANDOM.nextBytes(secretBytes)
      val applicationSecret = UUID.randomUUID() + "-" + BaseEncoding.base64().encode(secretBytes)
      val namespaceDir = new File(dependenciesRootDir, podNameAndNamespace.namespace)
      val applicationDir = new File(namespaceDir, podNameAndNamespace.name)
      DIRECTORIES_LOCK.synchronized {
        if (!applicationDir.exists()) {
          if (!applicationDir.mkdirs()) {
            throw new SparkException("Failed to create dependencies directory for application" +
              s" at ${applicationDir.getAbsolutePath}")
          }
        }
      }
      val jarsTgz = new File(applicationDir, "jars.tgz")
      // TODO encrypt the written data with the secret.
      Utils.tryWithResource(new FileOutputStream(jarsTgz)) { ByteStreams.copy(jars, _) }
      val filesTgz = new File(applicationDir, "files.tgz")
      Utils.tryWithResource(new FileOutputStream(filesTgz)) { ByteStreams.copy(files, _) }
      SPARK_APPLICATION_DEPENDENCIES_LOCK.synchronized {
        sparkApplicationDependencies(applicationSecret) = SparkApplicationDependencies(
          podNameAndNamespace,
          jarsTgz,
          filesTgz,
          kubernetesCredentials)
      }
      applicationSecret
    } catch {
      case e: Throwable =>
        // Revert the registered driver if we failed for any reason, most likely from disk ops
        registeredDrivers.remove(podNameAndNamespace)
        throw e
    }
  }

  override def downloadJars(applicationSecret: String): StreamingOutput = {
    appFileToStreamingOutput(applicationSecret, _.jarsTgz)
  }

  override def downloadFiles(applicationSecret: String): StreamingOutput = {
    appFileToStreamingOutput(applicationSecret, _.filesTgz)
  }

  private def appFileToStreamingOutput(
      applicationSecret: String,
      dependency: (SparkApplicationDependencies => File)): StreamingOutput = {
    val applicationDependencies = SPARK_APPLICATION_DEPENDENCIES_LOCK.synchronized {
      sparkApplicationDependencies
        .get(applicationSecret)
        .getOrElse(throw new SparkException("No application found for the provided token."))
    }
    new StreamingOutput {
      override def write(outputStream: OutputStream) = {
        Files.copy(dependency(applicationDependencies), outputStream)
      }
    }
  }
}

private case class PodNameAndNamespace(name: String, namespace: String)
private case class SparkApplicationDependencies(
  driverPodNameAndNamespace: PodNameAndNamespace,
  jarsTgz: File,
  filesTgz: File,
  kubernetesCredentials: KubernetesCredentials)
