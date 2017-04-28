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

import java.io.{ByteArrayOutputStream, File}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.google.common.io.Files
import okhttp3.RequestBody
import okio.Okio
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import retrofit2.{Call, Response}

import org.apache.spark.{SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.CompressionUtils
import org.apache.spark.deploy.rest.kubernetes.v2.{ResourceStagingServiceRetrofit, RetrofitClientFactory, StagedResourceIdentifier}
import org.apache.spark.util.Utils

private[spark] class SubmittedDependencyUploaderSuite extends SparkFunSuite with BeforeAndAfter {
  import SubmittedDependencyUploaderSuite.createTempFile

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(new DefaultScalaModule)
  private val APP_ID = "app-id"
  private val LABELS = Map("label1" -> "label1value", "label2" -> "label2value")
  private val NAMESPACE = "namespace"
  private val STAGING_SERVER_URI = "http://localhost:8000"
  private val LOCAL_JARS = Seq(createTempFile("jar"), createTempFile("jar"))
  private val JARS = Seq("hdfs://localhost:9000/jars/jar1.jar",
    s"file://${LOCAL_JARS.head}",
    LOCAL_JARS(1))
  private val LOCAL_FILES = Seq(createTempFile("txt"))
  private val FILES = Seq("hdfs://localhost:9000/files/file1.txt",
    LOCAL_FILES.head)
  private val TRUSTSTORE_FILE = new File(createTempFile(".jks"))
  private val TRUSTSTORE_PASSWORD = "trustStorePassword"
  private val TRUSTSTORE_TYPE = "jks"
  private val STAGING_SERVER_SSL_OPTIONS = SSLOptions(
    enabled = true,
    trustStore = Some(TRUSTSTORE_FILE),
    trustStorePassword = Some(TRUSTSTORE_PASSWORD),
    trustStoreType = Some(TRUSTSTORE_TYPE))
  private var retrofitClientFactory: RetrofitClientFactory = _
  private var retrofitClient: ResourceStagingServiceRetrofit = _

  private var dependencyManagerUnderTest: SubmittedDependencyUploader = _

  before {
    retrofitClientFactory = mock[RetrofitClientFactory]
    retrofitClient = mock[ResourceStagingServiceRetrofit]
    Mockito.when(
      retrofitClientFactory.createRetrofitClient(
        STAGING_SERVER_URI, classOf[ResourceStagingServiceRetrofit], STAGING_SERVER_SSL_OPTIONS))
      .thenReturn(retrofitClient)
    dependencyManagerUnderTest = new SubmittedDependencyUploaderImpl(
      APP_ID,
      LABELS,
      NAMESPACE,
      STAGING_SERVER_URI,
      JARS,
      FILES,
      STAGING_SERVER_SSL_OPTIONS,
      retrofitClientFactory)
  }

  test("Uploading jars should contact the staging server with the appropriate parameters") {
    val capturingArgumentsAnswer = new UploadDependenciesArgumentsCapturingAnswer(
      StagedResourceIdentifier("resourceId", "resourceSecret"))
    Mockito.when(retrofitClient.uploadResources(any(), any(), any(), any()))
      .thenAnswer(capturingArgumentsAnswer)
    dependencyManagerUnderTest.uploadJars()
    testUploadSendsCorrectFiles(LOCAL_JARS, capturingArgumentsAnswer)
  }

  test("Uploading files should contact the staging server with the appropriate parameters") {
    val capturingArgumentsAnswer = new UploadDependenciesArgumentsCapturingAnswer(
      StagedResourceIdentifier("resourceId", "resourceSecret"))
    Mockito.when(retrofitClient.uploadResources(any(), any(), any(), any()))
      .thenAnswer(capturingArgumentsAnswer)
    dependencyManagerUnderTest.uploadFiles()
    testUploadSendsCorrectFiles(LOCAL_FILES, capturingArgumentsAnswer)
  }

  private def testUploadSendsCorrectFiles(
      expectedFiles: Seq[String],
      capturingArgumentsAnswer: UploadDependenciesArgumentsCapturingAnswer) = {
    val requestLabelsBytes = requestBodyBytes(capturingArgumentsAnswer.podLabelsArg)
    val requestLabelsString = new String(requestLabelsBytes, Charsets.UTF_8)
    val requestLabelsMap = OBJECT_MAPPER.readValue(
      requestLabelsString, classOf[Map[String, String]])
    assert(requestLabelsMap === LABELS)
    val requestNamespaceBytes = requestBodyBytes(capturingArgumentsAnswer.podNamespaceArg)
    val requestNamespaceString = new String(requestNamespaceBytes, Charsets.UTF_8)
    assert(requestNamespaceString === NAMESPACE)
    val localJarsTarStream = new ByteArrayOutputStream()
    CompressionUtils.writeTarGzipToStream(localJarsTarStream, expectedFiles)
    val requestResourceBytes = requestBodyBytes(capturingArgumentsAnswer.podResourcesArg)
    val jarBytes = localJarsTarStream.toByteArray
    assert(requestResourceBytes.sameElements(jarBytes))
  }

  private def requestBodyBytes(requestBody: RequestBody): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { outputStream =>
      Utils.tryWithResource(Okio.sink(outputStream)) { sink =>
        Utils.tryWithResource(Okio.buffer(sink)) { bufferedSink =>
          try {
            requestBody.writeTo(bufferedSink)
          } finally {
            bufferedSink.flush()
          }
        }
      }
      outputStream.toByteArray
    }
  }
}

private class UploadDependenciesArgumentsCapturingAnswer(returnValue: StagedResourceIdentifier)
    extends Answer[Call[StagedResourceIdentifier]] {

  var podLabelsArg: RequestBody = _
  var podNamespaceArg: RequestBody = _
  var podResourcesArg: RequestBody = _
  var kubernetesCredentialsArg: RequestBody = _

  override def answer(invocationOnMock: InvocationOnMock): Call[StagedResourceIdentifier] = {
    podLabelsArg = invocationOnMock.getArgumentAt(0, classOf[RequestBody])
    podNamespaceArg = invocationOnMock.getArgumentAt(1, classOf[RequestBody])
    podResourcesArg = invocationOnMock.getArgumentAt(2, classOf[RequestBody])
    kubernetesCredentialsArg = invocationOnMock.getArgumentAt(3, classOf[RequestBody])
    val responseCall = mock[Call[StagedResourceIdentifier]]
    Mockito.when(responseCall.execute()).thenReturn(Response.success(returnValue))
    responseCall
  }
}

private object SubmittedDependencyUploaderSuite {
  def createTempFile(extension: String): String = {
    val dir = Utils.createTempDir()
    val file = new File(dir, s"${UUID.randomUUID().toString}.$extension")
    Files.write(UUID.randomUUID().toString, file, Charsets.UTF_8)
    file.getAbsolutePath
  }
}
