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

import java.io.{File, FileInputStream, StringWriter}
import java.security.KeyStore

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite, SSLOptions}
import org.apache.spark.deploy.kubernetes.SSLUtils
import org.apache.spark.util.Utils

class DependencyServerSslOptionsProviderSuite extends SparkFunSuite with BeforeAndAfter {

  private val SSL_TEMP_DIR = Utils.createTempDir(namePrefix = "dependency-server-ssl-test")
  private val KEYSTORE_FILE = new File(SSL_TEMP_DIR, "keyStore.jks")

  private var sparkConf: SparkConf = _
  private var sslOptionsProvider: DependencyServerSslOptionsProvider = _

  before {
    sparkConf = new SparkConf(true)
    sslOptionsProvider = new DependencyServerSslOptionsProviderImpl(sparkConf)
  }

  test("Default SparkConf does not have TLS enabled.") {
    assert(sslOptionsProvider.getSslOptions === SSLOptions())
    assert(!sslOptionsProvider.getSslOptions.enabled)
  }

  test("Setting keyStore, key password, and key field directly.") {
    sparkConf.set("spark.ssl.kubernetes.dependencyserver.enabled", "true")
      .set("spark.ssl.kubernetes.dependencyserver.keyStore", KEYSTORE_FILE.getAbsolutePath)
      .set("spark.ssl.kubernetes.dependencyserver.keyStorePassword", "keyStorePassword")
      .set("spark.ssl.kubernetes.dependencyserver.keyPassword", "keyPassword")
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.enabled, "SSL should be enabled.")
    assert(sslOptions.keyStore.map(_.getAbsolutePath) === Some(KEYSTORE_FILE.getAbsolutePath),
      "Incorrect keyStore path or it was not set.")
    assert(sslOptions.keyStorePassword === Some("keyStorePassword"),
      "Incorrect keyStore password or it was not set.")
    assert(sslOptions.keyPassword === Some("keyPassword"),
      "Incorrect key password or it was not set.")
  }

  test("Setting key and certificate pem files should write an appropriate keyStore.") {
    val (keyPemFile, certPemFile) = SSLUtils.generateKeyCertPemPair("127.0.0.1")
    sparkConf.set("spark.ssl.kubernetes.dependencyserver.enabled", "true")
      .set("spark.ssl.kubernetes.dependencyserver.keyPem", keyPemFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.dependencyserver.serverCertPem", certPemFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.dependencyserver.keyStorePassword", "keyStorePassword")
      .set("spark.ssl.kubernetes.dependencyserver.keyPassword", "keyPassword")
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.enabled, "SSL should be enabled.")
    assert(sslOptions.keyStore.isDefined, "KeyStore should be defined.")
    sslOptions.keyStore.foreach { keyStoreFile =>
      val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
      Utils.tryWithResource(new FileInputStream(keyStoreFile)) {
        keyStore.load(_, "keyStorePassword".toCharArray)
      }
      val key = keyStore.getKey("key", "keyPassword".toCharArray)
      compareJcaPemObjectToFileString(key, keyPemFile)
      val certificate = keyStore.getCertificateChain("key")(0)
      compareJcaPemObjectToFileString(certificate, certPemFile)
    }
  }

  test("Using password files should read from the appropriate locations.") {
    val keyStorePasswordFile = new File(SSL_TEMP_DIR, "keyStorePassword.txt")
    Files.write("keyStorePassword", keyStorePasswordFile, Charsets.UTF_8)
    val keyPasswordFile = new File(SSL_TEMP_DIR, "keyPassword.txt")
    Files.write("keyPassword", keyPasswordFile, Charsets.UTF_8)
    sparkConf.set("spark.ssl.kubernetes.dependencyserver.enabled", "true")
      .set("spark.ssl.kubernetes.dependencyserver.keyStore", KEYSTORE_FILE.getAbsolutePath)
      .set("spark.ssl.kubernetes.dependencyserver.keyStorePasswordFile",
        keyStorePasswordFile.getAbsolutePath)
      .set("spark.ssl.kubernetes.dependencyserver.keyPasswordFile", keyPasswordFile.getAbsolutePath)
    val sslOptions = sslOptionsProvider.getSslOptions
    assert(sslOptions.keyStorePassword === Some("keyStorePassword"),
      "Incorrect keyStore password or it was not set.")
    assert(sslOptions.keyPassword === Some("keyPassword"),
      "Incorrect key password or it was not set.")
  }

  private def compareJcaPemObjectToFileString(pemObject: Any, pemFile: File): Unit = {
    Utils.tryWithResource(new StringWriter()) { stringWriter =>
      Utils.tryWithResource(new JcaPEMWriter(stringWriter)) { pemWriter =>
        pemWriter.writeObject(pemObject)
      }
      val pemFileAsString = Files.toString(pemFile, Charsets.UTF_8)
      assert(stringWriter.toString === pemFileAsString)
    }
  }
}
