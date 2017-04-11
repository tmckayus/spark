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

import okhttp3.{RequestBody, ResponseBody}
import retrofit2.Call
import retrofit2.http.{Multipart, Streaming}

import org.apache.spark.deploy.rest.KubernetesCredentials

/**
 * Retrofit-compatible variant of {@link KubernetesSparkDependencyService}. For documentation on
 * how to use this service, see the aforementioned JAX-RS based interface.
 */
private[spark] trait KubernetesSparkDependencyServiceRetrofit {

  @Multipart
  @retrofit2.http.PUT("/api/dependencies")
  def uploadDependencies(
      @retrofit2.http.Query("driverPodName") driverPodName: String,
      @retrofit2.http.Query("driverPodNamespace") driverPodNamespace: String,
      @retrofit2.http.Part("jars") jars: RequestBody,
      @retrofit2.http.Part("files") files: RequestBody,
      @retrofit2.http.Part("kubernetesCredentials")
          kubernetesCredentials: RequestBody): Call[String]

  @Streaming
  @retrofit2.http.GET("/api/dependencies/jars")
  def downloadJars(
      @retrofit2.http.Header("Authorization") applicationSecret: String): Call[ResponseBody]

  @Streaming
  @retrofit2.http.GET("/api/dependencies/files")
  def downloadFiles(
      @retrofit2.http.Header("Authorization") applicationSecret: String): Call[ResponseBody]
}
