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

import java.io.File
import java.util.Collections

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, OwnerReferenceBuilder, PodBuilder}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.Utils

/**
 * Submission client for launching Spark applications on Kubernetes clusters.
 *
 * This class is responsible for instantiating Kubernetes resources that allow a Spark driver to
 * run in a pod on the Kubernetes cluster with the Spark configurations specified by spark-submit.
 * The API of this class makes it such that much of the specific behavior can be stubbed for
 * testing; most of the detailed logic must be dependency-injected when constructing an instance
 * of this client. Therefore the submission process is designed to be as modular as possible,
 * where different steps of submission should be factored out into separate classes.
 */
private[spark] class Client(
      mainClass: String,
      sparkConf: SparkConf,
      appArgs: Array[String],
      mainAppResource: String,
      // TODO consider a more concise hierachy for these components that groups related functions
      // together. The modules themselves make sense but we could have higher-order compositions
      // of them - for example, an InitContainerManager can contain all of the sub-modules relating
      // to the init-container bootstrap.
      kubernetesClientProvider: SubmissionKubernetesClientProvider,
      submittedDepsUploaderProvider: SubmittedDependencyUploaderProvider,
      submittedDepsSecretBuilder: SubmittedDependencySecretBuilder,
      submittedDepsConfPluginProvider: SubmittedDependencyInitContainerConfigPluginProvider,
      submittedDepsVolumesPluginProvider: SubmittedDependencyInitContainerVolumesPluginProvider,
      initContainerConfigMapBuilderProvider: SparkInitContainerConfigMapBuilderProvider,
      initContainerBootstrapProvider: SparkPodInitContainerBootstrapProvider,
      containerLocalizedFilesResolverProvider: ContainerLocalizedFilesResolverProvider,
      executorInitContainerConfiguration: ExecutorInitContainerConfiguration)
    extends Logging {

  private val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
  private val master = resolveK8sMaster(sparkConf.get("spark.master"))
  private val launchTime = System.currentTimeMillis
  private val appName = sparkConf.getOption("spark.app.name")
    .getOrElse("spark")
  private val kubernetesAppId = s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
  private val driverDockerImage = sparkConf.get(DRIVER_DOCKER_IMAGE)
  private val maybeStagingServerUri = sparkConf.get(RESOURCE_STAGING_SERVER_URI)
  private val driverMemoryMb = sparkConf.get(org.apache.spark.internal.config.DRIVER_MEMORY)
  private val memoryOverheadMb = sparkConf
    .get(KUBERNETES_DRIVER_MEMORY_OVERHEAD)
    .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * driverMemoryMb).toInt,
      MEMORY_OVERHEAD_MIN))
  private val driverContainerMemoryWithOverhead = driverMemoryMb + memoryOverheadMb
  private val customLabels = sparkConf.get(KUBERNETES_DRIVER_LABELS)
  private val customAnnotations = sparkConf.get(KUBERNETES_DRIVER_ANNOTATIONS)
  private val sparkJars = sparkConf.getOption("spark.jars")
    .map(_.split(","))
    .getOrElse(Array.empty[String]) ++
    Option(mainAppResource)
      .filterNot(_ == SparkLauncher.NO_RESOURCE)
      .toSeq

  private val sparkFiles = sparkConf.getOption("spark.files")
    .map(_.split(","))
    .getOrElse(Array.empty[String])
  private val driverExtraClasspath = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_CLASS_PATH)
  private val driverJavaOptions = sparkConf.get(
    org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)

  def run(): Unit = {
    validateNoDuplicateFileNames(sparkJars)
    validateNoDuplicateFileNames(sparkFiles)
    val parsedCustomLabels = parseKeyValuePairs(customLabels, KUBERNETES_DRIVER_LABELS.key,
      "labels")
    require(!parsedCustomLabels.contains(SPARK_APP_ID_LABEL), s"Label with key " +
      s" $SPARK_APP_ID_LABEL is not allowed as it is reserved for Spark bookkeeping operations.")
    require(!parsedCustomLabels.contains(SPARK_APP_NAME_LABEL), s"Label with key" +
      s" $SPARK_APP_NAME_LABEL is not allowed as it is reserved for Spark bookkeeping operations.")
    val allLabels = parsedCustomLabels ++
      Map(SPARK_APP_ID_LABEL -> kubernetesAppId, SPARK_APP_NAME_LABEL -> appName)
    val parsedCustomAnnotations = parseKeyValuePairs(
      customAnnotations,
      KUBERNETES_DRIVER_ANNOTATIONS.key,
      "annotations")
    Utils.tryWithResource(kubernetesClientProvider.get) { kubernetesClient =>
      val driverExtraClasspathEnv = driverExtraClasspath.map { classPath =>
        new EnvVarBuilder()
          .withName(ENV_SUBMIT_EXTRA_CLASSPATH)
          .withValue(classPath)
          .build()
      }
      val driverContainer = new ContainerBuilder()
        .withName(DRIVER_CONTAINER_NAME)
        .withImage(driverDockerImage)
        .withImagePullPolicy("IfNotPresent")
        .addToEnv(driverExtraClasspathEnv.toSeq: _*)
        .addNewEnv()
          .withName(ENV_DRIVER_MEMORY)
          .withValue(driverContainerMemoryWithOverhead + "m")
          .endEnv()
        .addNewEnv()
          .withName(ENV_DRIVER_MAIN_CLASS)
          .withValue(mainClass)
          .endEnv()
        .addNewEnv()
          .withName(ENV_DRIVER_ARGS)
          .withValue(appArgs.mkString(" "))
          .endEnv()
        .build()
      val basePod = new PodBuilder()
        .withNewMetadata()
          .withName(kubernetesAppId)
          .addToLabels(allLabels.asJava)
          .addToAnnotations(parsedCustomAnnotations.asJava)
          .endMetadata()
        .withNewSpec()
          .addToContainers(driverContainer)
          .endSpec()

      val maybeSubmittedDependencyUploader = maybeStagingServerUri.map { stagingServerUri =>
        submittedDepsUploaderProvider.getSubmittedDependencyUploader(
          kubernetesAppId,
          stagingServerUri,
          allLabels,
          namespace,
          sparkJars,
          sparkFiles)
      }
      val maybeJarsResourceId = maybeSubmittedDependencyUploader.map(_.uploadJars())
      val maybeFilesResourceId = maybeSubmittedDependencyUploader.map(_.uploadFiles())
      val maybeSubmittedDependenciesSecret = for {
        jarsResourceId <- maybeJarsResourceId
        filesResourceid <- maybeFilesResourceId
      } yield {
        submittedDepsSecretBuilder.buildInitContainerSecret(
          kubernetesAppId, jarsResourceId.resourceSecret, filesResourceid.resourceSecret)
      }
      val maybeSubmittedDependenciesVolumesPlugin = maybeSubmittedDependenciesSecret.map {
        submittedDepsVolumesPluginProvider.getInitContainerVolumesPlugin
      }
      val maybeSubmittedDependencyConfigPlugin = for {
        stagingServerUri <- maybeStagingServerUri
        jarsResourceId <- maybeJarsResourceId
        filesResourceId <- maybeFilesResourceId
      } yield {
        submittedDepsConfPluginProvider.getSubmittedDependenciesInitContainerConfigPlugin(
          stagingServerUri, jarsResourceId.resourceId, filesResourceId.resourceId)
      }
      val initContainerConfigMap = initContainerConfigMapBuilderProvider
          .getInitConfigMapBuilder(
                kubernetesAppId, sparkJars, sparkFiles, maybeSubmittedDependencyConfigPlugin)
          .buildInitContainerConfigMap()
      val initContainerBootstrap = initContainerBootstrapProvider.getInitContainerBootstrap(
        initContainerConfigMap.configMap,
        initContainerConfigMap.configMapKey,
        maybeSubmittedDependenciesVolumesPlugin)
      val podWithInitContainer = initContainerBootstrap.bootstrapInitContainerAndVolumes(
        driverContainer.getName, basePod)

      val nonDriverPodKubernetesResources = Seq(initContainerConfigMap.configMap) ++
        maybeSubmittedDependenciesSecret.toSeq

      val containerLocalizedFilesResolver = containerLocalizedFilesResolverProvider
        .getContainerLocalizedFilesResolver(sparkJars, sparkFiles)
      val resolvedSparkJars = containerLocalizedFilesResolver.resolveSubmittedSparkJars()
      val resolvedSparkFiles = containerLocalizedFilesResolver.resolveSubmittedSparkFiles()
      val resolvedSparkConf = executorInitContainerConfiguration
          .configureSparkConfForExecutorInitContainer(
              maybeSubmittedDependenciesSecret,
              initContainerConfigMap.configMapKey,
              initContainerConfigMap.configMap,
              sparkConf)
      if (resolvedSparkJars.nonEmpty) {
        resolvedSparkConf.set("spark.jars", resolvedSparkJars.mkString(","))
      }
      if (resolvedSparkFiles.nonEmpty) {
        resolvedSparkConf.set("spark.files", resolvedSparkFiles.mkString(","))
      }
      resolvedSparkConf.set(KUBERNETES_DRIVER_POD_NAME, kubernetesAppId)
      resolvedSparkConf.set("spark.app.id", kubernetesAppId)
      // We don't need this anymore since we just set the JVM options on the environment
      resolvedSparkConf.remove(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)
      resolvedSparkConf.get(KUBERNETES_SUBMIT_OAUTH_TOKEN).foreach { _ =>
        resolvedSparkConf.set(KUBERNETES_SUBMIT_OAUTH_TOKEN.key, "<present_but_redacted>")
      }
      resolvedSparkConf.get(KUBERNETES_DRIVER_OAUTH_TOKEN).foreach { _ =>
        resolvedSparkConf.set(KUBERNETES_DRIVER_OAUTH_TOKEN.key, "<present_but_redacted>")
      }
      val resolvedLocalClasspath = containerLocalizedFilesResolver
        .resolveSubmittedAndRemoteSparkJars()
      val resolvedDriverJavaOpts = resolvedSparkConf.getAll.map {
        case (confKey, confValue) => s"-D$confKey=$confValue"
      }.mkString(" ") + driverJavaOptions.map(" " + _).getOrElse("")
      val resolvedDriverPod = podWithInitContainer.editSpec()
        .editMatchingContainer(new ContainerNameEqualityPredicate(driverContainer.getName))
          .addNewEnv()
            .withName(ENV_MOUNTED_CLASSPATH)
            .withValue(resolvedLocalClasspath.mkString(File.pathSeparator))
            .endEnv()
          .addNewEnv()
            .withName(ENV_DRIVER_JAVA_OPTS)
            .withValue(resolvedDriverJavaOpts)
            .endEnv()
          .endContainer()
        .endSpec()
        .build()
      val createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
      try {
        val driverPodOwnerReference = new OwnerReferenceBuilder()
          .withName(createdDriverPod.getMetadata.getName)
          .withApiVersion(createdDriverPod.getApiVersion)
          .withUid(createdDriverPod.getMetadata.getUid)
          .withKind(createdDriverPod.getKind)
          .withController(true)
          .build()
        nonDriverPodKubernetesResources.foreach { resource =>
          val originalMetadata = resource.getMetadata
          originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
        }
        kubernetesClient.resourceList(nonDriverPodKubernetesResources: _*).createOrReplace()
      } catch {
        case e: Throwable =>
          kubernetesClient.pods().delete(createdDriverPod)
          throw e
      }
    }
  }

  private def validateNoDuplicateFileNames(allFiles: Seq[String]): Unit = {
    val fileNamesToUris = allFiles.map { file =>
      (new File(Utils.resolveURI(file).getPath).getName, file)
    }
    fileNamesToUris.groupBy(_._1).foreach {
      case (fileName, urisWithFileName) =>
        require(urisWithFileName.size == 1, "Cannot add multiple files with the same name, but" +
          s" file name $fileName is shared by all of these URIs: $urisWithFileName")
    }
  }

  private def parseKeyValuePairs(
      maybeKeyValues: Option[String],
      configKey: String,
      keyValueType: String): Map[String, String] = {
    maybeKeyValues.map(keyValues => {
      keyValues.split(",").map(_.trim).filterNot(_.isEmpty).map(keyValue => {
        keyValue.split("=", 2).toSeq match {
          case Seq(k, v) =>
            (k, v)
          case _ =>
            throw new SparkException(s"Custom $keyValueType set by $configKey must be a" +
              s" comma-separated list of key-value pairs, with format <key>=<value>." +
              s" Got value: $keyValue. All values: $keyValues")
        }
      }).toMap
    }).getOrElse(Map.empty[String, String])
  }
}
