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

/**
 * Retrofit-compatible variant of {@link ResourceStagingService}. For documentation on
 * how to use this service, see the aforementioned JAX-RS based interface.
 */
private[spark] trait ResourceStagingServiceRetrofit {

  @Multipart
  @retrofit2.http.PUT("/api/resources/upload")
  def uploadResources(
      @retrofit2.http.Part("podLabels") podLabels: RequestBody,
      @retrofit2.http.Part("podNamespace") podNamespace: RequestBody,
      @retrofit2.http.Part("resources") resources: RequestBody,
      @retrofit2.http.Part("kubernetesCredentials")
          kubernetesCredentials: RequestBody): Call[String]

  @Streaming
  @retrofit2.http.GET("/api/resources/download")
  def downloadResources(
      @retrofit2.http.Header("Authorization") applicationSecret: String): Call[ResponseBody]
}
