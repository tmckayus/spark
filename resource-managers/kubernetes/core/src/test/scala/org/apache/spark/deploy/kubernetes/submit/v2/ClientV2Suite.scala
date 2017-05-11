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

<<<<<<< HEAD
||||||| merged common ancestors
import java.io.File

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, Container, DoneablePod, HasMetadata, Pod, PodBuilder, PodList, Secret, SecretBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.Matchers.{any, anyVararg, argThat, startsWith, eq => mockitoEq}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
=======
import java.io.File

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, Container, DoneablePod, HasMetadata, Pod, PodBuilder, PodList, Secret, SecretBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.Matchers.{any, anyVararg, argThat, eq => mockitoEq, startsWith}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
>>>>>>> apache-spark-on-k8s/branch-2.1-kubernetes
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite

class ClientV2Suite extends SparkFunSuite with BeforeAndAfter {
  // TODO
}
