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

import java.util.UUID
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.ByteStreams
import okhttp3.{RequestBody, ResponseBody}
import org.scalatest.BeforeAndAfter
import retrofit2.Call

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.deploy.rest.kubernetes.v1.KubernetesCredentials
import org.apache.spark.util.Utils

/**
 * Tests for KubernetesSparkDependencyServer and its APIs. Note that this is not an end-to-end
 * integration test, and as such does not upload and download files in tar.gz as would be done
 * in production. Thus we use the retrofit clients directly despite the fact that in practice
 * we would likely want to create an opinionated abstraction on top of the retrofit client; we
 * can test this abstraction layer separately, however. This test is mainly for checking that
 * we've configured the Jetty server correctly and that the endpoints reached over HTTP can
 * receive streamed uploads and can stream downloads.
 */
class KubernetesSparkDependencyServerSuite extends SparkFunSuite with BeforeAndAfter {

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)

  private val serviceImpl = new KubernetesSparkDependencyServiceImpl(Utils.createTempDir())
  private val sslOptionsProvider = new SettableReferenceSslOptionsProvider()
  private val server = new KubernetesSparkDependencyServer(10021, serviceImpl, sslOptionsProvider)

  after {
    server.stop()
  }

  test("Accept file and jar uploads and downloads") {
    server.start()
    runUploadAndDownload(SSLOptions())
  }

  test("Enable SSL on the server") {
    val (keyStore, trustStore) = SSLUtils.generateKeyStoreTrustStorePair(
      "127.0.0.1", "changeit", "changeit", "changeit")
    val sslOptions = SSLOptions(
      enabled = true,
      keyStore = Some(keyStore),
      keyStorePassword = Some("changeit"),
      keyPassword = Some("changeit"),
      trustStore = Some(trustStore),
      trustStorePassword = Some("changeit"))
    sslOptionsProvider.setOptions(sslOptions)
    server.start()
    runUploadAndDownload(sslOptions)
  }

  private def runUploadAndDownload(sslOptions: SSLOptions): Unit = {
    val scheme = if (sslOptions.enabled) "https" else "http"
    val retrofitService = RetrofitUtils.createRetrofitClient(s"$scheme://127.0.0.1:10021/",
      classOf[KubernetesSparkDependencyServiceRetrofit], sslOptions)
    val jarsBytes = Array[Byte](1, 2, 3, 4)
    val filesBytes = Array[Byte](5, 6, 7)
    val jarsRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.MULTIPART_FORM_DATA), jarsBytes)
    val filesRequestBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.MULTIPART_FORM_DATA), filesBytes)
    val kubernetesCredentials = KubernetesCredentials(Some("token"), Some("ca-cert"), None, None)
    val kubernetesCredentialsString = OBJECT_MAPPER.writer()
      .writeValueAsString(kubernetesCredentials)
    val kubernetesCredentialsBody = RequestBody.create(
      okhttp3.MediaType.parse(MediaType.APPLICATION_JSON), kubernetesCredentialsString)
    val uploadResponse = retrofitService.uploadDependencies(
      UUID.randomUUID().toString,
      UUID.randomUUID().toString,
      jarsRequestBody,
      filesRequestBody,
      kubernetesCredentialsBody)
    val secret = getTypedResponseResult(uploadResponse)

    checkResponseBodyBytesMatches(retrofitService.downloadJars(secret),
      jarsBytes)
    checkResponseBodyBytesMatches(retrofitService.downloadFiles(secret),
      filesBytes)
  }

  private def getTypedResponseResult[T](call: Call[T]): T = {
    val response = call.execute()
    assert(response.code() >= 200 && response.code() < 300, Option(response.errorBody())
      .map(_.string())
      .getOrElse("Error executing HTTP request, but error body was not provided."))
    val callResult = response.body()
    assert(callResult != null)
    callResult
  }

  private def checkResponseBodyBytesMatches(call: Call[ResponseBody], bytes: Array[Byte]): Unit = {
    val responseBody = getTypedResponseResult(call)
    val downloadedBytes = ByteStreams.toByteArray(responseBody.byteStream())
    assert(downloadedBytes.toSeq === bytes)
  }
}

private class SettableReferenceSslOptionsProvider extends DependencyServerSslOptionsProvider {
  private var options = SSLOptions()

  def setOptions(newOptions: SSLOptions): Unit = {
    this.options = newOptions
  }

  override def getSslOptions: SSLOptions = options
}
