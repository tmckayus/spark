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

import java.io.InputStream
import javax.ws.rs.{Consumes, GET, HeaderParam, Path, Produces, PUT, QueryParam}
import javax.ws.rs.core.{MediaType, StreamingOutput}

import org.glassfish.jersey.media.multipart.FormDataParam

import org.apache.spark.deploy.rest.kubernetes.v1.KubernetesCredentials

/**
 * Service that receives application data that can be received later on. This is primarily used
 * in the context of Spark, but the concept is generic enough to be used for arbitrary applications.
 * The use case is to have a place for Kubernetes application submitters to bootstrap dynamic,
 * heavyweight application data for pods. Application submitters may have data stored on their
 * local disks that they want to provide to the pods they create through the API server. ConfigMaps
 * are one way to provide this data, but the data in ConfigMaps are stored in etcd which cannot
 * maintain data in the hundreds of megabytes in size.<br>
 * <br>
 * The general use case is for an application submitter to ship the dependencies to the server via
 * {@link uploadDependencies}; the application submitter will then receive a unique secure token.
 * The application submitter then ought to convert the token into a secret, and use this secret in
 * a pod that fetches the uploaded dependencies via {@link downloadJars}, {@link downloadFiles}, and
 * {@link getKubernetesCredentials}.
 */
@Path("/")
private[spark] trait KubernetesSparkDependencyService {

  /**
   * Register an application with the dependency service, so that the driver pod can retrieve them
   * when it runs.
   *
   * @param driverPodName Name of the driver pod.
   * @param driverPodNamespace Namespace for the driver pod.
   * @param jars Application jars to upload, compacted together in tar + gzip format. The tarball
   *             should contain the jar files laid out in a flat hierarchy, without any directories.
   *             We take a stream here to avoid holding these entirely in memory.
   * @param files Application files to upload, compacted together in tar + gzip format. THe tarball
   *              should contain the files laid out in a flat hierarchy, without any directories.
   *              We take a stream here to avoid holding these entirely in memory.
   * @param kubernetesCredentials These credentials are primarily used to monitor the progress of
   *                              the application. When the application shuts down normally, shuts
   *                              down abnormally and does not restart, or fails to start entirely,
   *                              the data uploaded through this endpoint is cleared.
   * @return A unique token that should be provided when retrieving these dependencies later.
   */
  @PUT
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.TEXT_PLAIN))
  @Path("/dependencies")
  def uploadDependencies(
      @QueryParam("driverPodName") driverPodName: String,
      @QueryParam("driverPodNamespace") driverPodNamespace: String,
      @FormDataParam("jars") jars: InputStream,
      @FormDataParam("files") files: InputStream,
      @FormDataParam("kubernetesCredentials") kubernetesCredentials: KubernetesCredentials)
      : String

  /**
   * Download an application's jars. The jars are provided as a stream, where the stream's
   * underlying data matches the stream that was uploaded in {@link uploadDependencies}.
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Path("/dependencies/jars")
  def downloadJars(
      @HeaderParam("Authorization") applicationSecret: String): StreamingOutput

  /**
   * Download an application's files. The jars are provided as a stream, where the stream's
   * underlying data matches the stream that was uploaded in {@link uploadDependencies}.
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
  @Path("/dependencies/files")
  def downloadFiles(
      @HeaderParam("Authorization") applicationSecret: String): StreamingOutput

  /**
   * Retrieve the Kubernetes credentials being used to monitor the Spark application.
   */
  @GET
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/dependencies/credentials")
  def getKubernetesCredentials(
      @HeaderParam("Authorization") applicationSecret: String): KubernetesCredentials
}
