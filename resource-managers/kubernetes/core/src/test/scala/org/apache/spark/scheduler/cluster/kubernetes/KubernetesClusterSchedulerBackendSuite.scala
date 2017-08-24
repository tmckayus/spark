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
package org.apache.spark.scheduler.cluster.kubernetes

import java.util.concurrent.ScheduledExecutorService

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, MixedOperation, NonNamespaceOperation, PodResource, Resource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.TaskSchedulerImpl

private[spark] class KubernetesClusterSchedulerBackendSuite
    extends SparkFunSuite with BeforeAndAfter {

  private val APP_ID = "test-spark-app"
  private val DRIVER_POD_NAME = "spark-driver-pod"
  private val NAMESPACE = "test-namespace"
  private val SPARK_DRIVER_HOST = "localhost"
  private val SPARK_DRIVER_PORT = 7077

  private type PODS = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
  private type LABELLED_PODS = FilterWatchListDeletable[
        Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]]
  private type IN_NAMESPACE_PODS = NonNamespaceOperation[
        Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]

  @Mock
  private var sparkContext: SparkContext = _

  @Mock
  private var taskSchedulerImpl: TaskSchedulerImpl = _

  @Mock
  private var allocatorExecutor: ScheduledExecutorService = _

  @Mock
  private var executorPodFactory: ExecutorPodFactory = _

  @Mock
  private var shuffleManager: KubernetesExternalShuffleManager = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithLabelOperations: LABELLED_PODS = _

  @Mock
  private var podsInNamespace: IN_NAMESPACE_PODS = _

  @Mock
  private var podsWithDriverName: PodResource[Pod, DoneablePod] = _

  @Mock
  private var rpcEnv: RpcEnv = _

  @Mock
  private var executorPodsWatch: Watch = _

  private var sparkConf: SparkConf = _
  private var executorPodsWatcherArgument: ArgumentCaptor[Watcher[Pod]] = _

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(DRIVER_POD_NAME)
      .addToLabels(SPARK_APP_ID_LABEL, APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .endMetadata()
    .build()

  before {
    MockitoAnnotations.initMocks(this)
    sparkConf = new SparkConf()
        .set("spark.app.id", APP_ID)
        .set(KUBERNETES_DRIVER_POD_NAME, DRIVER_POD_NAME)
        .set(KUBERNETES_NAMESPACE, NAMESPACE)
        .set("spark.driver.host", SPARK_DRIVER_HOST)
        .set("spark.driver.port", SPARK_DRIVER_PORT.toString)
    executorPodsWatcherArgument = ArgumentCaptor.forClass(classOf[Watcher[Pod]])
    when(sparkContext.conf).thenReturn(sparkConf)
    when(taskSchedulerImpl.sc).thenReturn(sparkContext)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, APP_ID)).thenReturn(podsWithLabelOperations)
    when(podsWithLabelOperations.watch(executorPodsWatcherArgument.capture()))
        .thenReturn(executorPodsWatch)
    when(podOperations.inNamespace(NAMESPACE)).thenReturn(podsInNamespace)
    when(podsInNamespace.withName(DRIVER_POD_NAME)).thenReturn(podsWithDriverName)
    when(podsWithDriverName.get()).thenReturn(driverPod)
  }

  test("Basic lifecycle expectations when starting and stopping the scheduler.") {
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    verify(shuffleManager).start(APP_ID)
    assert(executorPodsWatcherArgument.getValue != null)
    scheduler.stop()
    verify(shuffleManager).stop()
    verify(executorPodsWatch).close()
  }

  private def newSchedulerBackend(externalShuffle: Boolean): KubernetesClusterSchedulerBackend = {
    new KubernetesClusterSchedulerBackend(
        taskSchedulerImpl,
        rpcEnv,
        executorPodFactory,
        if (externalShuffle) Some(shuffleManager) else None,
        kubernetesClient,
        allocatorExecutor)
  }

}
