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

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

/**
 * Unit, scala-level tests for KubernetesSparkDependencyServiceImpl. The coverage here
 * differs from that of KubernetesSparkDependencyServerSuite as here we invoke the
 * implementation methods directly as opposed to over HTTP.
 */
class KubernetesSparkDependencyServiceImplSuite extends SparkFunSuite with BeforeAndAfter {

  private val dependencyRootDir = Utils.createTempDir()
  private val serviceImpl = new KubernetesSparkDependencyServiceImpl(dependencyRootDir)
  private val jarsBytes = Array[Byte](1, 2, 3, 4)
  private val filesBytes = Array[Byte](5, 6, 7)
  private var podName: String = _
  private var podNamespace: String = _

  before {
    podName = UUID.randomUUID().toString
    podNamespace = UUID.randomUUID().toString
  }

  test("Uploads should write to a directory in the underlying disk") {
    // TODO
  }

}
