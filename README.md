25/09/05 18:29:52 WARN VersionInfoUtils: The AWS SDK for Java 1.x entered maintenance mode starting July 31, 2024 and will reach end of support on December 31, 2025. For more information, see https://aws.amazon.com/blogs/developer/the-aws-sdk-for-java-1-x-is-in-maintenance-mode-effective-july-31-2024/
You can print where on the file system the AWS SDK for Java 1.x core runtime is located by setting the AWS_JAVA_V1_PRINT_LOCATION environment variable or aws.java.v1.printLocation system property to 'true'.
This message can be disabled by setting the AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT environment variable or aws.java.v1.disableDeprecationAnnouncement system property to 'true'.
The AWS SDK for Java 1.x is being used here:
at java.base/java.lang.Thread.getStackTrace(Thread.java:1619)
at com.amazonaws.util.VersionInfoUtils.printDeprecationAnnouncement(VersionInfoUtils.java:81)
at com.amazonaws.util.VersionInfoUtils.<clinit>(VersionInfoUtils.java:59)
at com.amazonaws.internal.EC2ResourceFetcher.<clinit>(EC2ResourceFetcher.java:44)
at com.amazonaws.auth.ContainerCredentialsFetcher.getCredentialsResponse(ContainerCredentialsFetcher.java:41)
at com.amazonaws.auth.BaseCredentialsFetcher.fetchCredentials(BaseCredentialsFetcher.java:159)
at com.amazonaws.auth.BaseCredentialsFetcher.getCredentials(BaseCredentialsFetcher.java:100)
at com.amazonaws.auth.ContainerCredentialsProvider.getCredentials(ContainerCredentialsProvider.java:111)
at com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper.getCredentials(EC2ContainerCredentialsProviderWrapper.java:77)
at com.amazonaws.auth.AWSCredentialsProviderChain.getCredentials(AWSCredentialsProviderChain.java:118)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.auth.AWSCredentialsProviderChain.getCredentials(AWSCredentialsProviderChain.java:118)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.getCredentialsFromContext(AmazonHttpClient.java:1269)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.runBeforeRequestHandlers(AmazonHttpClient.java:845)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.doExecute(AmazonHttpClient.java:794)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.executeWithTimer(AmazonHttpClient.java:781)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.execute(AmazonHttpClient.java:755)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutor.access$500(AmazonHttpClient.java:715)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient$RequestExecutionBuilderImpl.execute(AmazonHttpClient.java:697)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:561)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.http.AmazonHttpClient.execute(AmazonHttpClient.java:541)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5558)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.getBucketRegionViaHeadRequest(AmazonS3Client.java:6539)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.fetchRegionFromCache(AmazonS3Client.java:6511)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5543)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.invoke(AmazonS3Client.java:5505)
at com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.AmazonS3Client.getObjectMetadata(AmazonS3Client.java:1403)
at com.amazon.ws.emr.hadoop.fs.s3.lite.call.GetObjectMetadataCall.perform(GetObjectMetadataCall.java:26)
at com.amazon.ws.emr.hadoop.fs.s3.lite.call.GetObjectMetadataCall.perform(GetObjectMetadataCall.java:12)
at com.amazon.ws.emr.hadoop.fs.s3.lite.executor.GlobalS3Executor$CallPerformer.call(GlobalS3Executor.java:114)
at com.amazon.ws.emr.hadoop.fs.s3.lite.executor.GlobalS3Executor.execute(GlobalS3Executor.java:141)
at com.amazon.ws.emr.hadoop.fs.s3.lite.AmazonS3LiteClient.invoke(AmazonS3LiteClient.java:196)
at com.amazon.ws.emr.hadoop.fs.s3.lite.AmazonS3LiteClient.invoke(AmazonS3LiteClient.java:191)
at com.amazon.ws.emr.hadoop.fs.s3.lite.AmazonS3LiteClient.getObjectMetadata(AmazonS3LiteClient.java:96)
at com.amazon.ws.emr.hadoop.fs.s3.lite.AbstractAmazonS3Lite.getObjectMetadata(AbstractAmazonS3Lite.java:43)
at com.amazon.ws.emr.hadoop.fs.s3n.Jets3tNativeFileSystemStore.getFileMetadataFromCacheOrS3(Jets3tNativeFileSystemStore.java:674)
at com.amazon.ws.emr.hadoop.fs.s3n.Jets3tNativeFileSystemStore.retrieveMetadata(Jets3tNativeFileSystemStore.java:326)
at com.amazon.ws.emr.hadoop.fs.s3n.S3NativeFileSystem.getFileStatus(S3NativeFileSystem.java:548)
at org.apache.hadoop.fs.Globber.getFileStatus(Globber.java:169)
at org.apache.hadoop.fs.Globber.doGlob(Globber.java:580)
at org.apache.hadoop.fs.Globber.glob(Globber.java:256)
at org.apache.hadoop.fs.FileSystem.globStatus(FileSystem.java:2287)
at com.amazon.ws.emr.hadoop.fs.EmrFileSystem.globStatus(EmrFileSystem.java:461)
at org.apache.spark.util.DependencyUtils$.resolveGlobPath(DependencyUtils.scala:318)
at org.apache.spark.util.DependencyUtils$.$anonfun$resolveGlobPaths$2(DependencyUtils.scala:273)
at org.apache.spark.util.DependencyUtils$.$anonfun$resolveGlobPaths$2$adapted(DependencyUtils.scala:271)
at scala.collection.TraversableLike.$anonfun$flatMap$1(TraversableLike.scala:293)
at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:36)
at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:33)
at scala.collection.mutable.WrappedArray.foreach(WrappedArray.scala:38)
at scala.collection.TraversableLike.flatMap(TraversableLike.scala:293)
at scala.collection.TraversableLike.flatMap$(TraversableLike.scala:290)
at scala.collection.AbstractTraversable.flatMap(Traversable.scala:108)
at org.apache.spark.util.DependencyUtils$.resolveGlobPaths(DependencyUtils.scala:271)
at org.apache.spark.deploy.SparkSubmit.$anonfun$prepareSubmitEnvironment$6(SparkSubmit.scala:403)
at scala.Option.map(Option.scala:230)
at org.apache.spark.deploy.SparkSubmit.prepareSubmitEnvironment(SparkSubmit.scala:403)
at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1053)
at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:200)
at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:223)
at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:92)
at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1212)
at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1221)
at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Files s3://entergy-govdatacore-dataeng-code-repo-dev/entergy-gov-data-core-code/glue_packages/mosaic.zip from /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip to /home/hadoop/mosaic.zip
Files s3://entergy-govdatacore-dataeng-code-repo-dev/entergy-gov-data-core-code/glue_packages/yaml.zip from /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/yaml.zip to /home/hadoop/yaml.zip
Files s3://entergy-govdatacore-dataeng-code-repo-dev/entergy-gov-data-core-code/glue_packages/sqlglot.zip from /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/sqlglot.zip to /home/hadoop/sqlglot.zip
25/09/05 18:29:54 INFO EMRParamSideChannel: Setting FGAC mode to false
25/09/05 18:29:54 INFO SparkContext: Running Spark version 3.5.5-amzn-0
25/09/05 18:29:54 INFO SparkContext: OS info Linux, 5.10.238-234.956.amzn2.aarch64, aarch64
25/09/05 18:29:54 INFO SparkContext: Java version 17.0.15
25/09/05 18:29:54 INFO ResourceUtils: ==============================================================
25/09/05 18:29:54 INFO ResourceUtils: No custom resources configured for spark.driver.
25/09/05 18:29:54 INFO ResourceUtils: ==============================================================
25/09/05 18:29:54 INFO SparkContext: Submitted application: eb_raw_ano_documents_file
25/09/05 18:29:54 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(executorType -> name: executorType, amount: 1, script: , vendor: , cores -> name: cores, amount: 2, script: , vendor: , memory -> name: memory, amount: 4096, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/09/05 18:29:54 INFO ResourceProfile: Limiting resource is cpus at 2 tasks per executor
25/09/05 18:29:54 INFO ResourceProfileManager: Added ResourceProfile id: 0
25/09/05 18:29:54 INFO ResourceProfile: User executor ResourceProfile created, executor resources: Map(executorType -> name: executorType, amount: 1, script: , vendor: , cores -> name: cores, amount: 2, script: , vendor: , memory -> name: memory, amount: 4096, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
25/09/05 18:29:54 INFO ResourceProfile: Limiting resource is cpus at 2 tasks per executor
25/09/05 18:29:54 INFO ResourceProfileManager: Added ResourceProfile id: 1
25/09/05 18:29:54 INFO SecurityManager: Changing view acls to: hadoop
25/09/05 18:29:54 INFO SecurityManager: Changing modify acls to: hadoop
25/09/05 18:29:54 INFO SecurityManager: Changing view acls groups to: 
25/09/05 18:29:54 INFO SecurityManager: Changing modify acls groups to: 
25/09/05 18:29:54 INFO SecurityManager: SecurityManager: authentication enabled; ui acls disabled; users with view permissions: hadoop; groups with view permissions: EMPTY; users with modify permissions: hadoop; groups with modify permissions: EMPTY
25/09/05 18:29:55 INFO Utils: Successfully started service 'sparkDriver' on port 42067.
25/09/05 18:29:55 INFO SparkEnv: Registering MapOutputTracker
25/09/05 18:29:55 INFO SparkEnv: Registering BlockManagerMaster
25/09/05 18:29:55 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
25/09/05 18:29:55 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
25/09/05 18:29:55 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/09/05 18:29:55 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-32b9cf66-00f9-47c3-a1dc-ef5799f19af4
25/09/05 18:29:55 INFO MemoryStore: MemoryStore started with capacity 8.2 GiB
25/09/05 18:29:55 INFO SparkEnv: Registering OutputCommitCoordinator
25/09/05 18:29:55 INFO SubResultCacheManager: Sub-result caches are disabled.
25/09/05 18:29:55 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
25/09/05 18:29:55 INFO Utils: Successfully started service 'SparkUI' on port 4040.
25/09/05 18:29:55 INFO SparkContext: Added file file:/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip at spark://[2600:1f12:dda:d200:24e6:bdae:866e:99f8]:42067/files/mosaic.zip with timestamp 1757096994867
25/09/05 18:29:55 INFO Utils: Copying /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip to /tmp/spark-124ea3a3-cbd4-41bb-8822-292fe1567b68/userFiles-8d80fa39-270a-414f-97c4-67611fc2f265/mosaic.zip
25/09/05 18:29:55 INFO SparkContext: Added file file:/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/yaml.zip at spark://[2600:1f12:dda:d200:24e6:bdae:866e:99f8]:42067/files/yaml.zip with timestamp 1757096994867
25/09/05 18:29:55 INFO Utils: Copying /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/yaml.zip to /tmp/spark-124ea3a3-cbd4-41bb-8822-292fe1567b68/userFiles-8d80fa39-270a-414f-97c4-67611fc2f265/yaml.zip
25/09/05 18:29:55 INFO SparkContext: Added file file:/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/sqlglot.zip at spark://[2600:1f12:dda:d200:24e6:bdae:866e:99f8]:42067/files/sqlglot.zip with timestamp 1757096994867
25/09/05 18:29:55 INFO Utils: Copying /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/sqlglot.zip to /tmp/spark-124ea3a3-cbd4-41bb-8822-292fe1567b68/userFiles-8d80fa39-270a-414f-97c4-67611fc2f265/sqlglot.zip
25/09/05 18:29:55 INFO Utils: Using initial executors = 3, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
25/09/05 18:29:55 INFO ExecutorContainerAllocator: Set total expected execs to {0=3}
25/09/05 18:29:55 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 33481.
25/09/05 18:29:55 INFO NettyBlockTransferService: Server created on [2600:1f12:dda:d200:24e6:bdae:866e:99f8] [fd00::2%eth0]:33481
25/09/05 18:29:55 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
25/09/05 18:29:55 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, [2600:1f12:dda:d200:24e6:bdae:866e:99f8], 33481, None)
25/09/05 18:29:55 INFO ExecutorContainerAllocator: Going to request 3 executors for ResourceProfile Id: 0, target: 3 already provisioned: 0.
25/09/05 18:29:55 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:24e6:bdae:866e:99f8]:33481 with 8.2 GiB RAM, BlockManagerId(driver, [2600:1f12:dda:d200:24e6:bdae:866e:99f8], 33481, None)
25/09/05 18:29:55 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, [2600:1f12:dda:d200:24e6:bdae:866e:99f8], 33481, None)
25/09/05 18:29:55 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, [2600:1f12:dda:d200:24e6:bdae:866e:99f8], 33481, None)
25/09/05 18:29:55 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(1, 2, 3)
25/09/05 18:29:55 INFO TimeBasedRotatingEventLogFilesWriter: rotationIntervalInSeconds = 300, eventFileMinSize = 1048576, maxFilesToRetain = 2
25/09/05 18:29:55 INFO TimeBasedRotatingEventLogFilesWriter: Logging events to file:/var/log/spark/apps/eventlog_v2_00fvc7lmvahf2g03/00fvc7lmvahf2g03.inprogress
25/09/05 18:29:56 INFO Utils: Using initial executors = 3, max of spark.dynamicAllocation.initialExecutors, spark.dynamicAllocation.minExecutors and spark.executor.instances
25/09/05 18:29:56 INFO ExecutorAllocationManager: Dynamic allocation is enabled without a shuffle service.
25/09/05 18:29:56 INFO ExecutorContainerAllocator: Set total expected execs to {0=3}
25/09/05 18:29:56 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(2 -> 5ccc8d92-36ba-4760-44ba-d3ea62b21990, 1 -> 62cc8d92-36ba-0384-4897-ffff9fa00864, 3 -> 7ecc8d92-36ba-4c88-f2c2-a0784de6fddf)
25/09/05 18:30:21 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 3 already provisioned: 1.
25/09/05 18:30:21 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(4, 5)
25/09/05 18:30:21 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(5 -> 52cc8d92-67e8-6423-0f4e-ff40a970b4cc, 4 -> 3acc8d92-67e8-6383-5f26-02af85ad7af5)
25/09/05 18:30:25 INFO EmrServerlessClusterSchedulerBackend: SchedulerBackend is ready for scheduling beginning after waiting maxRegisteredResourcesWaitingTime: 30000000000(ns)
25/09/05 18:30:26 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
25/09/05 18:30:26 INFO DefaultAWSCredentialsProviderFactory: Unable to create provider using constructor: DefaultAWSCredentialsProviderChain(java.net.URI, org.apache.hadoop.conf.Configuration)
25/09/05 18:30:26 INFO ClientConfigurationFactory: Set initial getObject socket timeout to 2000 ms.
25/09/05 18:30:26 INFO SharedState: Warehouse path is 's3://entergy-govdatacore-raw-dev/'.
25/09/05 18:30:28 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:30:28 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:30:28 INFO SparkContext: Starting job: collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884
25/09/05 18:30:28 INFO DAGScheduler: Got job 0 (collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884) with 1 output partitions
25/09/05 18:30:28 INFO DAGScheduler: Final stage: ResultStage 0 (collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884)
25/09/05 18:30:28 INFO DAGScheduler: Parents of final stage: List()
25/09/05 18:30:28 INFO DAGScheduler: Missing parents: List()
25/09/05 18:30:28 INFO DAGScheduler: Submitting ResultStage 0 (MapPartitionsRDD[2] at collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884), which has no missing parents
25/09/05 18:30:28 INFO ExecutorContainerAllocator: Set total expected execs to {0=1}
25/09/05 18:30:28 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 11.0 KiB, free 8.2 GiB)
25/09/05 18:30:28 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 6.0 KiB, actual size: 6.0 KiB, free 8.2 GiB)
25/09/05 18:30:28 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f12:dda:d200:24e6:bdae:866e:99f8]:33481 (size: 6.0 KiB, free: 8.2 GiB)
25/09/05 18:30:28 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1664
25/09/05 18:30:29 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[2] at collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884) (first 15 tasks are for partitions Vector(0))
25/09/05 18:30:29 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
25/09/05 18:30:44 WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
25/09/05 18:30:59 WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
25/09/05 18:31:14 WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
25/09/05 18:31:23 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ffb2:e4ad:4700:16b:44844
25/09/05 18:31:24 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:ffb2:e4ad:4700:16b:44850) with ID 2,  ResourceProfileId 0
25/09/05 18:31:24 INFO ExecutorMonitor: New executor 2 has registered (new total is 1)
25/09/05 18:31:24 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]:44805 with 2.1 GiB RAM, BlockManagerId(2, [2600:1f12:dda:d200:ffb2:e4ad:4700:16b], 44805, None)
25/09/05 18:31:24 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) ([2600:1f12:dda:d200:ffb2:e4ad:4700:16b], executor 2, partition 0, PROCESS_LOCAL, 9416 bytes) 
25/09/05 18:31:24 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]:44805 (size: 6.0 KiB, free: 2.1 GiB)
25/09/05 18:31:33 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 9127 ms on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b] (executor 2) (1/1)
25/09/05 18:31:33 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
25/09/05 18:31:33 INFO DAGScheduler: ResultStage 0 (collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884) finished in 64.656 s
25/09/05 18:31:33 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
25/09/05 18:31:33 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
25/09/05 18:31:33 INFO DAGScheduler: Job 0 finished: collect at /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py:884, took 64.703199 s
25/09/05 18:31:33 INFO SQLExecution: Generating and posting SparkListenerSQLExecutionObfuscatedInfo...
25/09/05 18:31:33 INFO SQLExecution: Posted SparkListenerSQLExecutionObfuscatedInfo in 3 ms
25/09/05 18:31:34 INFO JDBCRelation: Number of partitions: 32, WHERE clauses of these partitions: "document_id" < 405764 or "document_id" is null, "document_id" >= 405764 AND "document_id" < 708773, "document_id" >= 708773 AND "document_id" < 1011782, "document_id" >= 1011782 AND "document_id" < 1314791, "document_id" >= 1314791 AND "document_id" < 1617800, "document_id" >= 1617800 AND "document_id" < 1920809, "document_id" >= 1920809 AND "document_id" < 2223818, "document_id" >= 2223818 AND "document_id" < 2526827, "document_id" >= 2526827 AND "document_id" < 2829836, "document_id" >= 2829836 AND "document_id" < 3132845, "document_id" >= 3132845 AND "document_id" < 3435854, "document_id" >= 3435854 AND "document_id" < 3738863, "document_id" >= 3738863 AND "document_id" < 4041872, "document_id" >= 4041872 AND "document_id" < 4344881, "document_id" >= 4344881 AND "document_id" < 4647890, "document_id" >= 4647890 AND "document_id" < 4950899, "document_id" >= 4950899 AND "document_id" < 5253908, "document_id" >= 5253908 AND "document_id" < 5556917, "document_id" >= 5556917 AND "document_id" < 5859926, "document_id" >= 5859926 AND "document_id" < 6162935, "document_id" >= 6162935 AND "document_id" < 6465944, "document_id" >= 6465944 AND "document_id" < 6768953, "document_id" >= 6768953 AND "document_id" < 7071962, "document_id" >= 7071962 AND "document_id" < 7374971, "document_id" >= 7374971 AND "document_id" < 7677980, "document_id" >= 7677980 AND "document_id" < 7980989, "document_id" >= 7980989 AND "document_id" < 8283998, "document_id" >= 8283998 AND "document_id" < 8587007, "document_id" >= 8587007 AND "document_id" < 8890016, "document_id" >= 8890016 AND "document_id" < 9193025, "document_id" >= 9193025 AND "document_id" < 9496034, "document_id" >= 9496034
25/09/05 18:31:34 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f12:dda:d200:24e6:bdae:866e:99f8]:33481 in memory (size: 6.0 KiB, free: 8.2 GiB)
25/09/05 18:31:34 INFO BlockManagerInfo: Removed broadcast_0_piece0 on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]:44805 in memory (size: 6.0 KiB, free: 2.1 GiB)
25/09/05 18:31:34 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
25/09/05 18:31:34 WARN GlueCatalogSessionExtensions: Fail to load catalog v1/catalogs/:: Forbidden: 331875467123 is not allowlisted to call GetCatalogExtension
25/09/05 18:31:34 INFO GlueUtil: Not use extensions: no catalog info
25/09/05 18:31:35 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
25/09/05 18:31:35 WARN GlueCatalogSessionExtensions: Fail to load catalog v1/catalogs/:: Forbidden: 331875467123 is not allowlisted to call GetCatalogExtension
25/09/05 18:31:35 INFO GlueUtil: Not use extensions: no catalog info
25/09/05 18:31:35 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:31:35 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:31:35 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
25/09/05 18:31:35 WARN GlueCatalogSessionExtensions: Fail to load catalog v1/catalogs/:: Forbidden: 331875467123 is not allowlisted to call GetCatalogExtension
25/09/05 18:31:35 INFO GlueUtil: Not use extensions: no catalog info
25/09/05 18:31:35 INFO GlueCatalog: Table properties set at catalog level through catalog properties: {}
25/09/05 18:31:35 INFO GlueUtil: Not use extensions: table properties {owner=hadoop}
25/09/05 18:31:35 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
25/09/05 18:31:35 INFO GlueCatalog: Table properties enforced at catalog level through catalog properties: {}
25/09/05 18:31:36 INFO SparkTable: Table eb_raw.ano_documents_file loaded Spark schema: StructType(StructField(pdm_file_id,IntegerType,true),StructField(document_id,IntegerType,true),StructField(file_name,StringType,true),StructField(file_date,TimestampType,true),StructField(file_type,StringType,true),StructField(file_size,DecimalType(18,0),true),StructField(stream_id,StringType,true),StructField(file_stream,BinaryType,true),StructField(pk_hash,StringType,true),StructField(edl_load_date,TimestampType,true))
25/09/05 18:31:36 INFO SparkWrite: Requesting 0 bytes advisory partition size for table eb_raw.ano_documents_file
25/09/05 18:31:36 INFO SparkWrite: Requesting UnspecifiedDistribution as write distribution for table eb_raw.ano_documents_file
25/09/05 18:31:36 INFO SparkWrite: Requesting [] as write ordering for table eb_raw.ano_documents_file
25/09/05 18:31:36 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:31:36 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:31:36 INFO RedshiftStrategy: No redshift relations, skip push down
25/09/05 18:31:36 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 32.0 KiB, free 8.2 GiB)
25/09/05 18:31:36 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.0 KiB, actual size: 3.0 KiB, free 8.2 GiB)
25/09/05 18:31:36 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:24e6:bdae:866e:99f8]:33481 (size: 3.0 KiB, free: 8.2 GiB)
25/09/05 18:31:36 INFO SparkContext: Created broadcast 1 from broadcast at SparkWrite.java:193
25/09/05 18:31:36 INFO AppendDataExec: Start processing data source write support: IcebergBatchWrite(table=eb_raw.ano_documents_file, format=PARQUET). The input RDD has 32 partitions.
25/09/05 18:31:36 INFO SparkContext: Starting job: createOrReplace at NativeMethodAccessorImpl.java:0
25/09/05 18:31:36 INFO DAGScheduler: Got job 1 (createOrReplace at NativeMethodAccessorImpl.java:0) with 32 output partitions
25/09/05 18:31:36 INFO DAGScheduler: Final stage: ResultStage 1 (createOrReplace at NativeMethodAccessorImpl.java:0)
25/09/05 18:31:36 INFO DAGScheduler: Parents of final stage: List()
25/09/05 18:31:36 INFO DAGScheduler: Missing parents: List()
25/09/05 18:31:36 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[5] at createOrReplace at NativeMethodAccessorImpl.java:0), which has no missing parents
25/09/05 18:31:36 INFO MemoryStore: Block broadcast_2 stored as values in memory (estimated size 29.9 KiB, free 8.2 GiB)
25/09/05 18:31:36 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 14.6 KiB, actual size: 14.6 KiB, free 8.2 GiB)
25/09/05 18:31:36 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:24e6:bdae:866e:99f8]:33481 (size: 14.6 KiB, free: 8.2 GiB)
25/09/05 18:31:36 INFO SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1664
25/09/05 18:31:36 INFO DAGScheduler: Submitting 32 missing tasks from ResultStage 1 (MapPartitionsRDD[5] at createOrReplace at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
25/09/05 18:31:36 INFO TaskSchedulerImpl: Adding task set 1.0 with 32 tasks resource profile 0
25/09/05 18:31:36 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) ([2600:1f12:dda:d200:ffb2:e4ad:4700:16b], executor 2, partition 0, PROCESS_LOCAL, 9465 bytes) 
25/09/05 18:31:36 INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 2) ([2600:1f12:dda:d200:ffb2:e4ad:4700:16b], executor 2, partition 1, PROCESS_LOCAL, 9468 bytes) 
25/09/05 18:31:36 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]:44805 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:31:50 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]:44805 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:32:06 INFO ExecutorContainerAllocator: Set total expected execs to {0=2}
25/09/05 18:32:06 INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged (new desired total will be 2 for resource profile id: 0)
25/09/05 18:32:06 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 2 already provisioned: 1.
25/09/05 18:32:06 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(6)
25/09/05 18:32:06 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(6 -> decc8d93-3545-7b00-6ceb-5199a5f038e9)
25/09/05 18:32:16 INFO ExecutorContainerAllocator: Set total expected execs to {0=4}
25/09/05 18:32:16 INFO ExecutorAllocationManager: Requesting 2 new executors because tasks are backlogged (new desired total will be 4 for resource profile id: 0)
25/09/05 18:32:16 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 4 already provisioned: 2.
25/09/05 18:32:16 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(7, 8)
25/09/05 18:32:16 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(8 -> 9ecc8d93-48f7-64b2-651b-79d3f0f11d6b, 7 -> 0ccc8d93-48f7-7ada-d261-9344356ceef7)
25/09/05 18:32:26 INFO ExecutorContainerAllocator: Set total expected execs to {0=8}
25/09/05 18:32:26 INFO ExecutorAllocationManager: Requesting 4 new executors because tasks are backlogged (new desired total will be 8 for resource profile id: 0)
25/09/05 18:32:26 INFO ExecutorContainerAllocator: Going to request 4 executors for ResourceProfile Id: 0, target: 8 already provisioned: 4.
25/09/05 18:32:26 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(9, 10, 11, 12)
25/09/05 18:32:26 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(12 -> 0ccc8d93-5caa-6fa0-398f-8110d6fd0407, 11 -> 20cc8d93-5caa-6e72-e3c2-726851aa23b8, 9 -> a2cc8d93-5caa-7828-70cb-dc117fe86c95, 10 -> f0cc8d93-5caa-3477-feb4-d9bf64f7347d)
25/09/05 18:32:31 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:32:31 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(13)
25/09/05 18:32:31 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(13 -> 6acc8d93-66be-44a1-f8dc-9998f44e55c2)
25/09/05 18:32:36 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:32:36 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(14, 15)
25/09/05 18:32:36 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(15 -> aacc8d93-70ac-dda6-f1d9-812f93a194eb, 14 -> f2cc8d93-70ac-cb7d-731c-b79f3be0c9a5)
25/09/05 18:32:47 INFO ExecutorContainerAllocator: Going to request 4 executors for ResourceProfile Id: 0, target: 8 already provisioned: 4.
25/09/05 18:32:47 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(16, 17, 18, 19)
25/09/05 18:32:47 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(18 -> 2acc8d93-846a-2422-63e3-1827e9410097, 17 -> 96cc8d93-846a-2fb5-2e0d-a818919104e4, 16 -> d8cc8d93-846a-14f1-52f4-ab343de1d958, 19 -> 3acc8d93-846a-17b5-d206-cee904cbd1f1)
25/09/05 18:32:52 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:32:52 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(20)
25/09/05 18:32:52 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(20 -> a4cc8d93-8e61-abbc-f2fc-a854dc9cafcc)
25/09/05 18:33:02 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:33:02 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(21, 22)
25/09/05 18:33:02 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(21 -> f2cc8d93-a215-ef7a-abdb-8292e13e4303, 22 -> 50cc8d93-a215-972d-c137-d5a3083d3ded)
25/09/05 18:33:07 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:33:07 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(23, 24)
25/09/05 18:33:07 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(24 -> 1ecc8d93-ac06-d99c-8fda-34b71d9bbbcb, 23 -> 28cc8d93-ac06-e5f1-d22c-562993da81e9)
25/09/05 18:33:12 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:33:12 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(25, 26)
25/09/05 18:33:12 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(26 -> 04cc8d93-b5f8-1918-dad3-717b243b579e, 25 -> 8ecc8d93-b5f8-1b96-831c-18f23b7ffc9c)
25/09/05 18:33:22 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:33:22 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(27, 28)
25/09/05 18:33:22 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(27 -> e2cc8d93-c9ad-89a5-f8b5-88bd6d7ac7cd, 28 -> 44cc8d93-c9ad-9b90-0e18-8a836f41591b)
25/09/05 18:33:24 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ffb2:e4ad:4700:16b:44858
25/09/05 18:33:32 INFO ExecutorContainerAllocator: Going to request 4 executors for ResourceProfile Id: 0, target: 8 already provisioned: 4.
25/09/05 18:33:32 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(29, 30, 31, 32)
25/09/05 18:33:32 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(30 -> d0cc8d93-dd63-1e26-927e-80f1542c0d08, 32 -> c8cc8d93-dd63-3620-14a5-657cf244ebee, 29 -> eccc8d93-dd63-62e8-3824-90f391a3a290, 31 -> f2cc8d93-dd63-3662-a1ac-6f54a7e28eb5)
25/09/05 18:33:42 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:33:42 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(33)
25/09/05 18:33:42 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(33 -> 34cc8d93-f117-a798-3da9-28751ed82ca7)
25/09/05 18:33:52 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 8 already provisioned: 6.
25/09/05 18:33:52 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(34, 35)
25/09/05 18:33:52 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(35 -> 44cc8d94-04ca-fbd1-cdda-7af5dd07372c, 34 -> 88cc8d94-04ca-e6ee-951f-1e3f235b3884)
25/09/05 18:34:02 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:34:02 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(36)
25/09/05 18:34:02 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(36 -> 18cc8d94-187d-c91f-d0b5-810f81bcb725)
25/09/05 18:34:17 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:34:17 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(37)
25/09/05 18:34:17 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(37 -> decc8d94-35f6-3e65-8a5e-11da11207f66)
25/09/05 18:34:18 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1:42464
25/09/05 18:34:18 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1:42480) with ID 18,  ResourceProfileId 0
25/09/05 18:34:18 INFO ExecutorMonitor: New executor 18 has registered (new total is 2)
25/09/05 18:34:18 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1]:34373 with 2.1 GiB RAM, BlockManagerId(18, [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1], 34373, None)
25/09/05 18:34:19 INFO TaskSetManager: Starting task 2.0 in stage 1.0 (TID 3) ([2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1], executor 18, partition 2, PROCESS_LOCAL, 9469 bytes) 
25/09/05 18:34:19 INFO TaskSetManager: Starting task 3.0 in stage 1.0 (TID 4) ([2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1], executor 18, partition 3, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:34:19 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1]:34373 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:34:27 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:34:27 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(38)
25/09/05 18:34:28 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(38 -> a6cc8d94-49a8-0e7b-e9ad-d00cf75a0893)
25/09/05 18:34:28 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1]:34373 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:34:53 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:34:53 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(39)
25/09/05 18:34:53 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(39 -> 7acc8d94-7aa9-fd8c-ed05-2c40062342fa)
25/09/05 18:34:54 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:249f:17d9:5d54:4de1:47962
25/09/05 18:34:54 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:249f:17d9:5d54:4de1:47966) with ID 28,  ResourceProfileId 0
25/09/05 18:34:54 INFO ExecutorMonitor: New executor 28 has registered (new total is 3)
25/09/05 18:34:54 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:249f:17d9:5d54:4de1]:39831 with 2.1 GiB RAM, BlockManagerId(28, [2600:1f12:dda:d200:249f:17d9:5d54:4de1], 39831, None)
25/09/05 18:34:54 INFO TaskSetManager: Starting task 4.0 in stage 1.0 (TID 5) ([2600:1f12:dda:d200:249f:17d9:5d54:4de1], executor 28, partition 4, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:34:54 INFO TaskSetManager: Starting task 5.0 in stage 1.0 (TID 6) ([2600:1f12:dda:d200:249f:17d9:5d54:4de1], executor 28, partition 5, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:34:55 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:249f:17d9:5d54:4de1]:39831 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:34:59 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:e9a1:d34f:94f5:5d57:48844
25/09/05 18:34:59 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:e9a1:d34f:94f5:5d57:48848) with ID 30,  ResourceProfileId 0
25/09/05 18:34:59 INFO ExecutorMonitor: New executor 30 has registered (new total is 4)
25/09/05 18:35:00 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57]:36563 with 2.1 GiB RAM, BlockManagerId(30, [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57], 36563, None)
25/09/05 18:35:00 INFO TaskSetManager: Starting task 6.0 in stage 1.0 (TID 7) ([2600:1f12:dda:d200:e9a1:d34f:94f5:5d57], executor 30, partition 6, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:00 INFO TaskSetManager: Starting task 7.0 in stage 1.0 (TID 8) ([2600:1f12:dda:d200:e9a1:d34f:94f5:5d57], executor 30, partition 7, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:00 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:c66c:4ea6:2c13:fd96:50788
25/09/05 18:35:00 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57]:36563 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:35:00 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:c66c:4ea6:2c13:fd96:50792) with ID 31,  ResourceProfileId 0
25/09/05 18:35:00 INFO ExecutorMonitor: New executor 31 has registered (new total is 5)
25/09/05 18:35:01 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96]:38985 with 2.1 GiB RAM, BlockManagerId(31, [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], 38985, None)
25/09/05 18:35:01 INFO TaskSetManager: Starting task 8.0 in stage 1.0 (TID 9) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], executor 31, partition 8, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:01 INFO TaskSetManager: Starting task 9.0 in stage 1.0 (TID 10) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], executor 31, partition 9, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:01 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96]:38985 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:35:03 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57]:36563 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:35:03 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:249f:17d9:5d54:4de1]:39831 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:35:04 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96]:38985 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:35:05 INFO TaskSetManager: Starting task 10.0 in stage 1.0 (TID 11) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], executor 31, partition 10, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:05 INFO TaskSetManager: Finished task 9.0 in stage 1.0 (TID 10) in 3900 ms on [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96] (executor 31) (1/32)
25/09/05 18:35:13 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 8 already provisioned: 7.
25/09/05 18:35:13 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(40)
25/09/05 18:35:13 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(40 -> bccc8d94-a1e4-2a86-4283-cf586e2c9aee)
25/09/05 18:35:20 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 2.
25/09/05 18:35:20 INFO DAGScheduler: Executor lost: 2 (epoch 0)
25/09/05 18:35:20 INFO BlockManagerMasterEndpoint: Trying to remove executor 2 from BlockManagerMaster.
25/09/05 18:35:20 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(2, [2600:1f12:dda:d200:ffb2:e4ad:4700:16b], 44805, None)
25/09/05 18:35:20 INFO BlockManagerMaster: Removed 2 successfully in removeExecutor
25/09/05 18:35:20 INFO DAGScheduler: Shuffle files lost for executor: 2 (epoch 0)
25/09/05 18:35:20 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:6438:49f8:b100:3a57:52668
25/09/05 18:35:20 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:6438:49f8:b100:3a57:52676) with ID 34,  ResourceProfileId 0
25/09/05 18:35:20 INFO ExecutorMonitor: New executor 34 has registered (new total is 6)
25/09/05 18:35:20 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:6438:49f8:b100:3a57]:35863 with 2.1 GiB RAM, BlockManagerId(34, [2600:1f12:dda:d200:6438:49f8:b100:3a57], 35863, None)
25/09/05 18:35:20 INFO TaskSetManager: Starting task 11.0 in stage 1.0 (TID 12) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 11, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:20 INFO TaskSetManager: Starting task 12.0 in stage 1.0 (TID 13) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 12, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:21 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:6438:49f8:b100:3a57]:35863 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:35:23 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:6438:49f8:b100:3a57]:35863 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:35:24 INFO TaskSetManager: Starting task 13.0 in stage 1.0 (TID 14) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 13, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:24 INFO TaskSetManager: Finished task 12.0 in stage 1.0 (TID 13) in 3564 ms on [2600:1f12:dda:d200:6438:49f8:b100:3a57] (executor 34) (2/32)
25/09/05 18:35:25 INFO TaskSetManager: Starting task 14.0 in stage 1.0 (TID 15) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 14, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:25 INFO TaskSetManager: Finished task 13.0 in stage 1.0 (TID 14) in 836 ms on [2600:1f12:dda:d200:6438:49f8:b100:3a57] (executor 34) (3/32)
25/09/05 18:35:26 WARN ExecutorContainerStoreImpl: Executor(s) 2 exited unexpectedly with exit code 137.
25/09/05 18:35:26 INFO TaskSetManager: Starting task 15.0 in stage 1.0 (TID 16) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 15, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:26 INFO TaskSetManager: Finished task 14.0 in stage 1.0 (TID 15) in 835 ms on [2600:1f12:dda:d200:6438:49f8:b100:3a57] (executor 34) (4/32)
25/09/05 18:35:26 INFO ExecutorContainerAllocator: Set total expected execs to {0=7}
25/09/05 18:35:26 ERROR TaskSchedulerImpl: Lost executor 2 on [2600:1f12:dda:d200:ffb2:e4ad:4700:16b]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:35:26 WARN TaskSetManager: Lost task 1.0 in stage 1.0 (TID 2) ([2600:1f12:dda:d200:ffb2:e4ad:4700:16b] executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:35:26 WARN TaskSetManager: Lost task 0.0 in stage 1.0 (TID 1) ([2600:1f12:dda:d200:ffb2:e4ad:4700:16b] executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:35:26 INFO ExecutorMonitor: Executor 2 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 1).
25/09/05 18:35:32 INFO TaskSetManager: Starting task 0.1 in stage 1.0 (TID 17) ([2600:1f12:dda:d200:6438:49f8:b100:3a57], executor 34, partition 0, PROCESS_LOCAL, 9465 bytes) 
25/09/05 18:35:32 INFO TaskSetManager: Finished task 11.0 in stage 1.0 (TID 12) in 11234 ms on [2600:1f12:dda:d200:6438:49f8:b100:3a57] (executor 34) (5/32)
25/09/05 18:35:43 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:35:43 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(41)
25/09/05 18:35:43 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(41 -> 2ecc8d94-dcb8-1dac-5058-4355935d48b7)
25/09/05 18:35:47 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:1909:75cc:cabe:a0a4:33320
25/09/05 18:35:47 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:1909:75cc:cabe:a0a4:33328) with ID 37,  ResourceProfileId 0
25/09/05 18:35:47 INFO ExecutorMonitor: New executor 37 has registered (new total is 6)
25/09/05 18:35:47 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:1909:75cc:cabe:a0a4]:34161 with 2.1 GiB RAM, BlockManagerId(37, [2600:1f12:dda:d200:1909:75cc:cabe:a0a4], 34161, None)
25/09/05 18:35:47 INFO TaskSetManager: Starting task 1.1 in stage 1.0 (TID 18) ([2600:1f12:dda:d200:1909:75cc:cabe:a0a4], executor 37, partition 1, PROCESS_LOCAL, 9468 bytes) 
25/09/05 18:35:47 INFO TaskSetManager: Starting task 16.0 in stage 1.0 (TID 19) ([2600:1f12:dda:d200:1909:75cc:cabe:a0a4], executor 37, partition 16, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:48 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:1909:75cc:cabe:a0a4]:34161 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:35:57 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 18.
25/09/05 18:35:57 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1:42482
25/09/05 18:35:57 INFO DAGScheduler: Executor lost: 18 (epoch 1)
25/09/05 18:35:57 INFO BlockManagerMasterEndpoint: Trying to remove executor 18 from BlockManagerMaster.
25/09/05 18:35:57 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(18, [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1], 34373, None)
25/09/05 18:35:57 INFO BlockManagerMaster: Removed 18 successfully in removeExecutor
25/09/05 18:35:57 INFO DAGScheduler: Shuffle files lost for executor: 18 (epoch 1)
25/09/05 18:35:57 INFO TaskSetManager: Starting task 17.0 in stage 1.0 (TID 20) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], executor 31, partition 17, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:35:57 INFO TaskSetManager: Finished task 8.0 in stage 1.0 (TID 9) in 56352 ms on [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96] (executor 31) (6/32)
25/09/05 18:36:03 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:36:03 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(42)
25/09/05 18:36:03 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(42 -> 54cc8d95-03f2-f5ec-8c11-cecccee780c1)
25/09/05 18:36:03 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:1909:75cc:cabe:a0a4]:34161 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:36:26 WARN ExecutorContainerStoreImpl: Executor(s) 18 exited unexpectedly with exit code 137.
25/09/05 18:36:26 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:36:26 ERROR TaskSchedulerImpl: Lost executor 18 on [2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:36:26 WARN TaskSetManager: Lost task 3.0 in stage 1.0 (TID 4) ([2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1] executor 18): ExecutorLostFailure (executor 18 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:36:26 WARN TaskSetManager: Lost task 2.0 in stage 1.0 (TID 3) ([2600:1f12:dda:d200:cd61:c1a1:1ae6:a3e1] executor 18): ExecutorLostFailure (executor 18 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:36:26 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(43)
25/09/05 18:36:26 INFO ExecutorMonitor: Executor 18 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 2).
25/09/05 18:36:26 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(43 -> 3ccc8d95-310e-bba6-24b9-8307d810191f)
25/09/05 18:36:48 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:36:48 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(44)
25/09/05 18:36:48 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(44 -> 7ecc8d95-5c35-6860-6d69-a9e092075fa0)
25/09/05 18:36:54 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:249f:17d9:5d54:4de1:47978
25/09/05 18:37:00 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:e9a1:d34f:94f5:5d57:48864
25/09/05 18:37:01 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:c66c:4ea6:2c13:fd96:50794
25/09/05 18:37:20 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:6438:49f8:b100:3a57:52690
25/09/05 18:37:34 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:89e6:7a82:2d27:13dd:41416
25/09/05 18:37:34 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:89e6:7a82:2d27:13dd:41428) with ID 42,  ResourceProfileId 0
25/09/05 18:37:34 INFO ExecutorMonitor: New executor 42 has registered (new total is 6)
25/09/05 18:37:34 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:89e6:7a82:2d27:13dd]:45163 with 2.1 GiB RAM, BlockManagerId(42, [2600:1f12:dda:d200:89e6:7a82:2d27:13dd], 45163, None)
25/09/05 18:37:35 INFO TaskSetManager: Starting task 2.1 in stage 1.0 (TID 21) ([2600:1f12:dda:d200:89e6:7a82:2d27:13dd], executor 42, partition 2, PROCESS_LOCAL, 9469 bytes) 
25/09/05 18:37:35 INFO TaskSetManager: Starting task 3.1 in stage 1.0 (TID 22) ([2600:1f12:dda:d200:89e6:7a82:2d27:13dd], executor 42, partition 3, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:37:35 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:89e6:7a82:2d27:13dd]:45163 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:37:43 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:89e6:7a82:2d27:13dd]:45163 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:37:47 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:1909:75cc:cabe:a0a4:33336
25/09/05 18:37:55 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 31.
25/09/05 18:37:55 INFO DAGScheduler: Executor lost: 31 (epoch 2)
25/09/05 18:37:55 INFO BlockManagerMasterEndpoint: Trying to remove executor 31 from BlockManagerMaster.
25/09/05 18:37:55 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(31, [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96], 38985, None)
25/09/05 18:37:55 INFO BlockManagerMaster: Removed 31 successfully in removeExecutor
25/09/05 18:37:55 INFO DAGScheduler: Shuffle files lost for executor: 31 (epoch 2)
25/09/05 18:38:16 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 30.
25/09/05 18:38:16 INFO DAGScheduler: Executor lost: 30 (epoch 3)
25/09/05 18:38:16 INFO BlockManagerMasterEndpoint: Trying to remove executor 30 from BlockManagerMaster.
25/09/05 18:38:16 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(30, [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57], 36563, None)
25/09/05 18:38:16 INFO BlockManagerMaster: Removed 30 successfully in removeExecutor
25/09/05 18:38:16 INFO DAGScheduler: Shuffle files lost for executor: 30 (epoch 3)
25/09/05 18:38:17 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:70dc:c5c0:b75d:2d70:57630
25/09/05 18:38:17 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:70dc:c5c0:b75d:2d70:57636) with ID 44,  ResourceProfileId 0
25/09/05 18:38:17 INFO ExecutorMonitor: New executor 44 has registered (new total is 7)
25/09/05 18:38:18 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70]:38059 with 2.1 GiB RAM, BlockManagerId(44, [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70], 38059, None)
25/09/05 18:38:18 INFO TaskSetManager: Starting task 18.0 in stage 1.0 (TID 23) ([2600:1f12:dda:d200:70dc:c5c0:b75d:2d70], executor 44, partition 18, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:38:18 INFO TaskSetManager: Starting task 19.0 in stage 1.0 (TID 24) ([2600:1f12:dda:d200:70dc:c5c0:b75d:2d70], executor 44, partition 19, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:38:18 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70]:38059 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:38:26 WARN ExecutorContainerStoreImpl: Executor(s) 30,31 exited unexpectedly with exit code 137.
25/09/05 18:38:26 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:38:26 ERROR TaskSchedulerImpl: Lost executor 31 on [2600:1f12:dda:d200:c66c:4ea6:2c13:fd96]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 WARN TaskSetManager: Lost task 10.0 in stage 1.0 (TID 11) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96] executor 31): ExecutorLostFailure (executor 31 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 WARN TaskSetManager: Lost task 17.0 in stage 1.0 (TID 20) ([2600:1f12:dda:d200:c66c:4ea6:2c13:fd96] executor 31): ExecutorLostFailure (executor 31 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 INFO ExecutorMonitor: Executor 31 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 3).
25/09/05 18:38:26 ERROR TaskSchedulerImpl: Lost executor 30 on [2600:1f12:dda:d200:e9a1:d34f:94f5:5d57]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 WARN TaskSetManager: Lost task 7.0 in stage 1.0 (TID 8) ([2600:1f12:dda:d200:e9a1:d34f:94f5:5d57] executor 30): ExecutorLostFailure (executor 30 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(45, 46)
25/09/05 18:38:26 WARN TaskSetManager: Lost task 6.0 in stage 1.0 (TID 7) ([2600:1f12:dda:d200:e9a1:d34f:94f5:5d57] executor 30): ExecutorLostFailure (executor 30 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:26 INFO ExecutorMonitor: Executor 30 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 4).
25/09/05 18:38:26 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(45 -> 4ecc8d96-1be0-a192-e0b9-73c60548baa8, 46 -> c6cc8d96-1be0-e6a1-bf71-ddb17b38b64c)
25/09/05 18:38:26 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70]:38059 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:38:48 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 28.
25/09/05 18:38:48 INFO DAGScheduler: Executor lost: 28 (epoch 4)
25/09/05 18:38:48 INFO BlockManagerMasterEndpoint: Trying to remove executor 28 from BlockManagerMaster.
25/09/05 18:38:48 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(28, [2600:1f12:dda:d200:249f:17d9:5d54:4de1], 39831, None)
25/09/05 18:38:48 INFO BlockManagerMaster: Removed 28 successfully in removeExecutor
25/09/05 18:38:48 INFO DAGScheduler: Shuffle files lost for executor: 28 (epoch 4)
25/09/05 18:38:48 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:38:48 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(47, 48)
25/09/05 18:38:48 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(48 -> 2ecc8d96-4706-c587-5f84-df9730d5737c, 47 -> 06cc8d96-4707-54be-441a-1a1bc3b8d304)
25/09/05 18:38:56 WARN ExecutorContainerStoreImpl: Executor(s) 28 exited unexpectedly with exit code 137.
25/09/05 18:38:56 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:38:56 ERROR TaskSchedulerImpl: Lost executor 28 on [2600:1f12:dda:d200:249f:17d9:5d54:4de1]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:56 WARN TaskSetManager: Lost task 4.0 in stage 1.0 (TID 5) ([2600:1f12:dda:d200:249f:17d9:5d54:4de1] executor 28): ExecutorLostFailure (executor 28 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:56 WARN TaskSetManager: Lost task 5.0 in stage 1.0 (TID 6) ([2600:1f12:dda:d200:249f:17d9:5d54:4de1] executor 28): ExecutorLostFailure (executor 28 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:38:56 INFO ExecutorMonitor: Executor 28 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 5).
25/09/05 18:38:56 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(49)
25/09/05 18:38:56 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(49 -> cecc8d96-56d2-5f28-875d-22b85a2036b5)
25/09/05 18:39:06 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 37.
25/09/05 18:39:06 INFO DAGScheduler: Executor lost: 37 (epoch 5)
25/09/05 18:39:06 INFO BlockManagerMasterEndpoint: Trying to remove executor 37 from BlockManagerMaster.
25/09/05 18:39:06 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(37, [2600:1f12:dda:d200:1909:75cc:cabe:a0a4], 34161, None)
25/09/05 18:39:06 INFO BlockManagerMaster: Removed 37 successfully in removeExecutor
25/09/05 18:39:06 INFO DAGScheduler: Shuffle files lost for executor: 37 (epoch 5)
25/09/05 18:39:13 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:39:13 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(50)
25/09/05 18:39:14 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(50 -> d8cc8d96-7832-f4e8-1eba-2ab18756903c)
25/09/05 18:39:19 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:39:19 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(51)
25/09/05 18:39:19 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(51 -> f4cc8d96-8222-f034-8b03-4ca2441e9d51)
25/09/05 18:39:26 WARN ExecutorContainerStoreImpl: Executor(s) 37 exited unexpectedly with exit code 137.
25/09/05 18:39:27 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:39:27 ERROR TaskSchedulerImpl: Lost executor 37 on [2600:1f12:dda:d200:1909:75cc:cabe:a0a4]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:27 WARN TaskSetManager: Lost task 16.0 in stage 1.0 (TID 19) ([2600:1f12:dda:d200:1909:75cc:cabe:a0a4] executor 37): ExecutorLostFailure (executor 37 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:27 WARN TaskSetManager: Lost task 1.1 in stage 1.0 (TID 18) ([2600:1f12:dda:d200:1909:75cc:cabe:a0a4] executor 37): ExecutorLostFailure (executor 37 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:27 INFO ExecutorMonitor: Executor 37 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 6).
25/09/05 18:39:27 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(52)
25/09/05 18:39:27 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(52 -> 0acc8d96-91ed-be4a-dcc7-dfcd3535178f)
25/09/05 18:39:29 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 42.
25/09/05 18:39:29 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:89e6:7a82:2d27:13dd:41434
25/09/05 18:39:29 INFO DAGScheduler: Executor lost: 42 (epoch 6)
25/09/05 18:39:29 INFO BlockManagerMasterEndpoint: Trying to remove executor 42 from BlockManagerMaster.
25/09/05 18:39:29 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(42, [2600:1f12:dda:d200:89e6:7a82:2d27:13dd], 45163, None)
25/09/05 18:39:29 INFO BlockManagerMaster: Removed 42 successfully in removeExecutor
25/09/05 18:39:29 INFO DAGScheduler: Shuffle files lost for executor: 42 (epoch 6)
25/09/05 18:39:31 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 44.
25/09/05 18:39:31 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:70dc:c5c0:b75d:2d70:57646
25/09/05 18:39:31 INFO DAGScheduler: Executor lost: 44 (epoch 7)
25/09/05 18:39:31 INFO BlockManagerMasterEndpoint: Trying to remove executor 44 from BlockManagerMaster.
25/09/05 18:39:31 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(44, [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70], 38059, None)
25/09/05 18:39:31 INFO BlockManagerMaster: Removed 44 successfully in removeExecutor
25/09/05 18:39:31 INFO DAGScheduler: Shuffle files lost for executor: 44 (epoch 7)
25/09/05 18:39:39 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:39:39 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(53, 54)
25/09/05 18:39:39 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(54 -> 06cc8d96-a989-5ef5-429f-857a25afc0eb, 53 -> 88cc8d96-a989-60c2-9e91-ceae62a4a7a6)
25/09/05 18:39:56 WARN ExecutorContainerStoreImpl: Executor(s) 42,44 exited unexpectedly with exit code 137.
25/09/05 18:39:57 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:39:57 ERROR TaskSchedulerImpl: Lost executor 42 on [2600:1f12:dda:d200:89e6:7a82:2d27:13dd]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 WARN TaskSetManager: Lost task 3.1 in stage 1.0 (TID 22) ([2600:1f12:dda:d200:89e6:7a82:2d27:13dd] executor 42): ExecutorLostFailure (executor 42 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 WARN TaskSetManager: Lost task 2.1 in stage 1.0 (TID 21) ([2600:1f12:dda:d200:89e6:7a82:2d27:13dd] executor 42): ExecutorLostFailure (executor 42 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 ERROR TaskSchedulerImpl: Lost executor 44 on [2600:1f12:dda:d200:70dc:c5c0:b75d:2d70]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 INFO ExecutorMonitor: Executor 42 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 7).
25/09/05 18:39:57 WARN TaskSetManager: Lost task 18.0 in stage 1.0 (TID 23) ([2600:1f12:dda:d200:70dc:c5c0:b75d:2d70] executor 44): ExecutorLostFailure (executor 44 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 WARN TaskSetManager: Lost task 19.0 in stage 1.0 (TID 24) ([2600:1f12:dda:d200:70dc:c5c0:b75d:2d70] executor 44): ExecutorLostFailure (executor 44 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:39:57 INFO ExecutorMonitor: Executor 44 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 8).
25/09/05 18:39:57 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(55, 56)
25/09/05 18:39:57 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(56 -> 50cc8d96-cce0-93d5-f2a5-8593033b0a31, 55 -> b6cc8d96-cce0-ebc2-a620-fa6f050e6041)
25/09/05 18:39:59 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:39:59 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(57)
25/09/05 18:39:59 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(57 -> 2ecc8d96-d0f0-7ad0-5e9f-00e1054a9e26)
25/09/05 18:40:04 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:40:04 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(58)
25/09/05 18:40:04 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(58 -> dccc8d96-dade-d068-0482-902cea59d9ba)
25/09/05 18:40:19 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ca0b:8444:83fa:fec2:39350
25/09/05 18:40:19 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:ca0b:8444:83fa:fec2:39358) with ID 48,  ResourceProfileId 0
25/09/05 18:40:19 INFO ExecutorMonitor: New executor 48 has registered (new total is 2)
25/09/05 18:40:19 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:40:19 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(59)
25/09/05 18:40:19 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:ca0b:8444:83fa:fec2]:44819 with 2.1 GiB RAM, BlockManagerId(48, [2600:1f12:dda:d200:ca0b:8444:83fa:fec2], 44819, None)
25/09/05 18:40:19 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(59 -> 2ccc8d96-f855-7aa5-e419-95a81f15eafc)
25/09/05 18:40:19 INFO TaskSetManager: Starting task 19.1 in stage 1.0 (TID 25) ([2600:1f12:dda:d200:ca0b:8444:83fa:fec2], executor 48, partition 19, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:40:19 INFO TaskSetManager: Starting task 18.1 in stage 1.0 (TID 26) ([2600:1f12:dda:d200:ca0b:8444:83fa:fec2], executor 48, partition 18, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:40:20 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:ca0b:8444:83fa:fec2]:44819 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:40:24 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:40:24 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(60, 61)
25/09/05 18:40:24 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(60 -> cacc8d97-023f-15db-7dea-28c1d552747b, 61 -> 2acc8d97-023f-5b74-dab5-ebd2bc6d906b)
25/09/05 18:40:34 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:ca0b:8444:83fa:fec2]:44819 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:40:44 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:40:44 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(62, 63)
25/09/05 18:40:44 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(63 -> c6cc8d97-297b-7b80-bd93-14335e2c5da6, 62 -> dccc8d97-297b-1832-c40b-4251f98901a6)
25/09/05 18:40:49 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:40:49 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(64)
25/09/05 18:40:49 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(64 -> 66cc8d97-336c-2c64-a44b-d281fbe88272)
25/09/05 18:41:02 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:61af:4fd9:a7d7:c67:55214
25/09/05 18:41:03 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:61af:4fd9:a7d7:c67:55226) with ID 52,  ResourceProfileId 0
25/09/05 18:41:03 INFO ExecutorMonitor: New executor 52 has registered (new total is 3)
25/09/05 18:41:03 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:61af:4fd9:a7d7:c67]:46519 with 2.1 GiB RAM, BlockManagerId(52, [2600:1f12:dda:d200:61af:4fd9:a7d7:c67], 46519, None)
25/09/05 18:41:03 INFO TaskSetManager: Starting task 2.2 in stage 1.0 (TID 27) ([2600:1f12:dda:d200:61af:4fd9:a7d7:c67], executor 52, partition 2, PROCESS_LOCAL, 9469 bytes) 
25/09/05 18:41:03 INFO TaskSetManager: Starting task 3.2 in stage 1.0 (TID 28) ([2600:1f12:dda:d200:61af:4fd9:a7d7:c67], executor 52, partition 3, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:41:03 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:61af:4fd9:a7d7:c67]:46519 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:41:09 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:41:09 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(65)
25/09/05 18:41:09 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(65 -> 72cc8d97-5aa8-e8dc-4d0d-529b7f9f9fce)
25/09/05 18:41:15 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:41:15 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(66, 67)
25/09/05 18:41:15 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(66 -> c4cc8d97-649a-1326-3846-84ac84fe3a7e, 67 -> 48cc8d97-649a-4083-1255-60e7486a4109)
25/09/05 18:41:17 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:61af:4fd9:a7d7:c67]:46519 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:41:26 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f:59706
25/09/05 18:41:27 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f:59708) with ID 56,  ResourceProfileId 0
25/09/05 18:41:27 INFO ExecutorMonitor: New executor 56 has registered (new total is 4)
25/09/05 18:41:27 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f]:43447 with 2.1 GiB RAM, BlockManagerId(56, [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f], 43447, None)
25/09/05 18:41:27 INFO TaskSetManager: Starting task 1.2 in stage 1.0 (TID 29) ([2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f], executor 56, partition 1, PROCESS_LOCAL, 9468 bytes) 
25/09/05 18:41:27 INFO TaskSetManager: Starting task 16.1 in stage 1.0 (TID 30) ([2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f], executor 56, partition 16, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:41:28 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f]:43447 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:41:30 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ca0b:8444:83fa:fec2:39372
25/09/05 18:41:30 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 48.
25/09/05 18:41:30 INFO DAGScheduler: Executor lost: 48 (epoch 8)
25/09/05 18:41:30 INFO BlockManagerMasterEndpoint: Trying to remove executor 48 from BlockManagerMaster.
25/09/05 18:41:30 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(48, [2600:1f12:dda:d200:ca0b:8444:83fa:fec2], 44819, None)
25/09/05 18:41:30 INFO BlockManagerMaster: Removed 48 successfully in removeExecutor
25/09/05 18:41:30 INFO DAGScheduler: Shuffle files lost for executor: 48 (epoch 8)
25/09/05 18:41:35 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:41:35 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(68)
25/09/05 18:41:35 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(68 -> b0cc8d97-8bd5-0fee-e7d5-4d8df40b400f)
25/09/05 18:41:39 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:41:39 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(69, 70)
25/09/05 18:41:39 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(69 -> 56cc8d97-93ce-c509-6d8c-33106dc02b97, 70 -> cccc8d97-93ce-e5eb-837b-49f167c8380e)
25/09/05 18:41:42 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f]:43447 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:41:54 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:41:54 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(71)
25/09/05 18:41:54 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(71 -> c2cc8d97-b147-4951-5294-63f1ff412b8a)
25/09/05 18:41:56 WARN ExecutorContainerStoreImpl: Executor(s) 48 exited unexpectedly with exit code 137.
25/09/05 18:41:57 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:41:57 ERROR TaskSchedulerImpl: Lost executor 48 on [2600:1f12:dda:d200:ca0b:8444:83fa:fec2]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:41:57 WARN TaskSetManager: Lost task 18.1 in stage 1.0 (TID 26) ([2600:1f12:dda:d200:ca0b:8444:83fa:fec2] executor 48): ExecutorLostFailure (executor 48 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:41:57 WARN TaskSetManager: Lost task 19.1 in stage 1.0 (TID 25) ([2600:1f12:dda:d200:ca0b:8444:83fa:fec2] executor 48): ExecutorLostFailure (executor 48 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:41:57 INFO ExecutorMonitor: Executor 48 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 9).
25/09/05 18:41:57 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(72)
25/09/05 18:41:57 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(72 -> a4cc8d97-b74f-9a77-f8b5-a423bcaca99c)
25/09/05 18:42:04 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:42:04 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(73)
25/09/05 18:42:04 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(73 -> aecc8d97-c528-64e5-e425-f7b9cef151ac)
25/09/05 18:42:14 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:61af:4fd9:a7d7:c67:55242
25/09/05 18:42:14 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 52.
25/09/05 18:42:14 INFO DAGScheduler: Executor lost: 52 (epoch 9)
25/09/05 18:42:14 INFO BlockManagerMasterEndpoint: Trying to remove executor 52 from BlockManagerMaster.
25/09/05 18:42:14 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(52, [2600:1f12:dda:d200:61af:4fd9:a7d7:c67], 46519, None)
25/09/05 18:42:14 INFO BlockManagerMaster: Removed 52 successfully in removeExecutor
25/09/05 18:42:14 INFO DAGScheduler: Shuffle files lost for executor: 52 (epoch 9)
25/09/05 18:42:14 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:42:14 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(74)
25/09/05 18:42:14 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(74 -> 7ccc8d97-d8da-67d8-ef24-cd65dc70cfdb)
25/09/05 18:42:24 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:42:24 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(75, 76)
25/09/05 18:42:24 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(76 -> 8ccc8d97-ec8c-6e40-269f-7048bb40cad3, 75 -> 9ccc8d97-ec8c-623e-7c48-98a0a951c815)
25/09/05 18:42:26 WARN ExecutorContainerStoreImpl: Executor(s) 52 exited unexpectedly with exit code 137.
25/09/05 18:42:26 ERROR TaskSchedulerImpl: Lost executor 52 on [2600:1f12:dda:d200:61af:4fd9:a7d7:c67]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:42:26 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:42:26 WARN TaskSetManager: Lost task 3.2 in stage 1.0 (TID 28) ([2600:1f12:dda:d200:61af:4fd9:a7d7:c67] executor 52): ExecutorLostFailure (executor 52 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:42:26 WARN TaskSetManager: Lost task 2.2 in stage 1.0 (TID 27) ([2600:1f12:dda:d200:61af:4fd9:a7d7:c67] executor 52): ExecutorLostFailure (executor 52 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:42:26 INFO ExecutorMonitor: Executor 52 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 10).
25/09/05 18:42:26 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(77)
25/09/05 18:42:26 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(77 -> 3acc8d97-f09f-4a33-993a-7fbdb50c57ba)
25/09/05 18:42:34 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:42:34 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(78)
25/09/05 18:42:34 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(78 -> 00cc8d98-0068-4353-87d8-bf81420433be)
25/09/05 18:42:49 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:42:49 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(79, 80)
25/09/05 18:42:49 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(80 -> b0cc8d98-1de6-2aab-ff1a-8770b6dffa1a, 79 -> 86cc8d98-1de6-0b3f-8fb1-20aeccb3d339)
25/09/05 18:42:54 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:42:54 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(81)
25/09/05 18:42:55 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(81 -> d8cc8d98-27d4-0ee9-1309-4592643e57c1)
25/09/05 18:43:05 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:43:05 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(82)
25/09/05 18:43:05 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(82 -> b2cc8d98-3b84-98c7-8dde-567a99f80c15)
25/09/05 18:43:07 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:cd7a:9511:e174:68f2:57400
25/09/05 18:43:07 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (2600:1f12:dda:d200:cd7a:9511:e174:68f2:57410) with ID 70,  ResourceProfileId 0
25/09/05 18:43:07 INFO ExecutorMonitor: New executor 70 has registered (new total is 3)
25/09/05 18:43:08 INFO BlockManagerMasterEndpoint: Registering block manager [2600:1f12:dda:d200:cd7a:9511:e174:68f2]:41243 with 2.1 GiB RAM, BlockManagerId(70, [2600:1f12:dda:d200:cd7a:9511:e174:68f2], 41243, None)
25/09/05 18:43:08 INFO TaskSetManager: Starting task 2.3 in stage 1.0 (TID 31) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2], executor 70, partition 2, PROCESS_LOCAL, 9469 bytes) 
25/09/05 18:43:08 INFO TaskSetManager: Starting task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2], executor 70, partition 3, PROCESS_LOCAL, 9470 bytes) 
25/09/05 18:43:08 INFO BlockManagerInfo: Added broadcast_2_piece0 in memory on [2600:1f12:dda:d200:cd7a:9511:e174:68f2]:41243 (size: 14.6 KiB, free: 2.1 GiB)
25/09/05 18:43:10 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:43:10 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(83, 84)
25/09/05 18:43:10 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(84 -> 62cc8d98-4572-b42f-8b98-4e6649f61f8e, 83 -> 12cc8d98-4572-c000-6ac5-065ab0cd5081)
25/09/05 18:43:17 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on [2600:1f12:dda:d200:cd7a:9511:e174:68f2]:41243 (size: 3.0 KiB, free: 2.1 GiB)
25/09/05 18:43:25 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:43:25 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(85, 86)
25/09/05 18:43:25 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(86 -> 52cc8d98-62eb-9b87-8572-24643efdb3ba, 85 -> 62cc8d98-62eb-98be-89cf-c10c917e6d91)
25/09/05 18:43:27 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f:59710
25/09/05 18:43:35 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:43:35 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(87, 88)
25/09/05 18:43:35 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(87 -> 90cc8d98-769e-304e-9d34-847e83bdfdc7, 88 -> 52cc8d98-769e-4fb7-4510-77de967b88c9)
25/09/05 18:43:45 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:43:45 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(89, 90)
25/09/05 18:43:45 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(90 -> 66cc8d98-8a50-1fd0-ce99-c20b476f2a2d, 89 -> 22cc8d98-8a50-2076-e596-a7a256123cdd)
25/09/05 18:44:00 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:44:00 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(91, 92)
25/09/05 18:44:00 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(92 -> 80cc8d98-a7d3-7741-e78c-1a6951148047, 91 -> 1ecc8d98-a7d3-43e2-a2a4-9a55c03d57c6)
25/09/05 18:44:10 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:44:10 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(93, 94)
25/09/05 18:44:10 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(94 -> 70cc8d98-bb86-b6c8-ff1d-b58b4be0cbf7, 93 -> 68cc8d98-bb86-e357-685d-4116ecb29c38)
25/09/05 18:44:25 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:44:25 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(95, 96)
25/09/05 18:44:25 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(95 -> 12cc8d98-d8ff-7459-e68c-198d1940c67c, 96 -> bccc8d98-d8ff-6bf2-a5de-d09fd28ec66f)
25/09/05 18:44:35 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:44:35 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(97, 98)
25/09/05 18:44:35 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(98 -> f4cc8d98-ecb2-6f0f-f35d-280d796d1f1d, 97 -> 86cc8d98-ecb2-5c68-a036-dc97eb1ae93c)
25/09/05 18:44:45 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:44:45 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(99)
25/09/05 18:44:45 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(99 -> cccc8d99-0067-9a6c-f457-7dbc41080f9d)
25/09/05 18:44:50 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:44:50 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(100)
25/09/05 18:44:51 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(100 -> f4cc8d99-0a56-be2e-b847-db9680cf113c)
25/09/05 18:45:01 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:01 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(101)
25/09/05 18:45:01 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(101 -> a4cc8d99-1e0b-09a6-ab89-30840b3bc994)
25/09/05 18:45:06 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:06 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(102)
25/09/05 18:45:06 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(102 -> 72cc8d99-27fa-a1eb-3df7-e99964fd61cb)
25/09/05 18:45:08 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: No executor found for 2600:1f12:dda:d200:cd7a:9511:e174:68f2:57420
25/09/05 18:45:11 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:11 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(103)
25/09/05 18:45:11 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(103 -> cecc8d99-31eb-aff4-d848-5d902ee38fa6)
25/09/05 18:45:16 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:16 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(104)
25/09/05 18:45:16 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(104 -> 72cc8d99-3bd8-3fec-7281-19586cef6373)
25/09/05 18:45:26 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:45:26 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(105, 106)
25/09/05 18:45:26 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(105 -> 54cc8d99-4f8a-1ba4-1766-9082ff897e27, 106 -> cecc8d99-4f8a-228c-19ca-6ef31523fa7f)
25/09/05 18:45:35 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:45:35 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(107, 108)
25/09/05 18:45:35 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(107 -> 66cc8d99-6148-db90-a1d8-08ec63e7c17e, 108 -> cccc8d99-6148-8255-a8d4-0334d0ff1abf)
25/09/05 18:45:38 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 56.
25/09/05 18:45:38 INFO DAGScheduler: Executor lost: 56 (epoch 10)
25/09/05 18:45:38 INFO BlockManagerMasterEndpoint: Trying to remove executor 56 from BlockManagerMaster.
25/09/05 18:45:38 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(56, [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f], 43447, None)
25/09/05 18:45:38 INFO BlockManagerMaster: Removed 56 successfully in removeExecutor
25/09/05 18:45:38 INFO DAGScheduler: Shuffle files lost for executor: 56 (epoch 10)
25/09/05 18:45:44 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Disabling executor 70.
25/09/05 18:45:44 INFO DAGScheduler: Executor lost: 70 (epoch 11)
25/09/05 18:45:44 INFO BlockManagerMasterEndpoint: Trying to remove executor 70 from BlockManagerMaster.
25/09/05 18:45:44 INFO BlockManagerMasterEndpoint: Removing block manager BlockManagerId(70, [2600:1f12:dda:d200:cd7a:9511:e174:68f2], 41243, None)
25/09/05 18:45:44 INFO BlockManagerMaster: Removed 70 successfully in removeExecutor
25/09/05 18:45:44 INFO DAGScheduler: Shuffle files lost for executor: 70 (epoch 11)
25/09/05 18:45:45 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:45 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(109)
25/09/05 18:45:45 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(109 -> 5ccc8d99-74fc-c90b-8561-57f750458159)
25/09/05 18:45:50 INFO ExecutorContainerAllocator: Going to request 1 executors for ResourceProfile Id: 0, target: 7 already provisioned: 6.
25/09/05 18:45:50 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(110)
25/09/05 18:45:50 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(110 -> b0cc8d99-7eeb-42b6-d917-bde6bc874f2c)
25/09/05 18:45:56 WARN ExecutorContainerStoreImpl: Executor(s) 70,56 exited unexpectedly with exit code 137.
25/09/05 18:45:56 INFO ExecutorContainerAllocator: Going to request 2 executors for ResourceProfile Id: 0, target: 7 already provisioned: 5.
25/09/05 18:45:56 ERROR TaskSchedulerImpl: Lost executor 56 on [2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 WARN TaskSetManager: Lost task 1.2 in stage 1.0 (TID 29) ([2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f] executor 56): ExecutorLostFailure (executor 56 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 WARN TaskSetManager: Lost task 16.1 in stage 1.0 (TID 30) ([2600:1f12:dda:d200:ed8b:bdd5:460d:7f8f] executor 56): ExecutorLostFailure (executor 56 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 ERROR TaskSchedulerImpl: Lost executor 70 on [2600:1f12:dda:d200:cd7a:9511:e174:68f2]: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 WARN TaskSetManager: Lost task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 INFO ExecutorMonitor: Executor 56 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 11).
25/09/05 18:45:56 INFO DefaultEmrServerlessRMClient: Creating containers with container role SPARK_EXECUTOR and keys: Set(111, 112)
25/09/05 18:45:56 ERROR TaskSetManager: Task 3 in stage 1.0 failed 4 times; aborting job
25/09/05 18:45:56 WARN TaskSetManager: Lost task 2.3 in stage 1.0 (TID 31) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
25/09/05 18:45:56 INFO ExecutorMonitor: Executor 70 is removed. Remove reason statistics: (gracefully decommissioned: 0, decommision unfinished: 0, driver killed: 0, unexpectedly exited: 12).
25/09/05 18:45:56 INFO TaskSchedulerImpl: Cancelling stage 1
25/09/05 18:45:56 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage cancelled: Job aborted due to stage failure: Task 3 in stage 1.0 failed 4 times, most recent failure: Lost task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
Driver stacktrace:
25/09/05 18:45:56 INFO TaskSchedulerImpl: Stage 1 was cancelled
25/09/05 18:45:56 INFO DAGScheduler: ResultStage 1 (createOrReplace at NativeMethodAccessorImpl.java:0) failed in 860.468 s due to Job aborted due to stage failure: Task 3 in stage 1.0 failed 4 times, most recent failure: Lost task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
Driver stacktrace:
25/09/05 18:45:56 INFO DAGScheduler: Job 1 failed: createOrReplace at NativeMethodAccessorImpl.java:0, took 860.475822 s
25/09/05 18:45:56 ERROR AppendDataExec: Data source write support IcebergBatchWrite(table=eb_raw.ano_documents_file, format=PARQUET) is aborting.
25/09/05 18:45:56 WARN SparkWrite: Skipping cleanup of written files
25/09/05 18:45:56 ERROR AppendDataExec: Data source write support IcebergBatchWrite(table=eb_raw.ano_documents_file, format=PARQUET) aborted.
25/09/05 18:45:56 INFO SQLExecution: Skipped SparkListenerSQLExecutionObfuscatedInfo event due to NON_EMPTY_ERROR.
25/09/05 18:45:56 ERROR Utils: Aborting task
org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 1.0 failed 4 times, most recent failure: Lost task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3083) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:3019) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:3018) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62) ~[scala-library-2.12.18.jar:?]
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55) ~[scala-library-2.12.18.jar:?]
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49) ~[scala-library-2.12.18.jar:?]
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:3018) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1324) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1324) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at scala.Option.foreach(Option.scala:407) ~[scala-library-2.12.18.jar:?]
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1324) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3301) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3235) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3224) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:1047) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2505) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:390) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2$(WriteToDataSourceV2Exec.scala:364) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.AppendDataExec.writeWithV2(WriteToDataSourceV2Exec.scala:230) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run(WriteToDataSourceV2Exec.scala:342) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run$(WriteToDataSourceV2Exec.scala:341) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.AppendDataExec.run(WriteToDataSourceV2Exec.scala:230) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result$lzycompute(V2CommandExec.scala:43) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result(V2CommandExec.scala:43) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.executeCollect(V2CommandExec.scala:49) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:126) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.executeQuery$1(SQLExecution.scala:157) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$10(SQLExecution.scala:220) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$9(SQLExecution.scala:220) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:405) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:219) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:901) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:83) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:74) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:123) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:114) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:521) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:77) ~[spark-sql-api_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:521) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:34) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:303) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:299) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:497) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:114) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:101) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:99) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:164) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.$anonfun$writeToTable$1(WriteToDataSourceV2Exec.scala:582) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1410) ~[spark-core_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.writeToTable(WriteToDataSourceV2Exec.scala:578) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.writeToTable$(WriteToDataSourceV2Exec.scala:572) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.AtomicReplaceTableAsSelectExec.writeToTable(WriteToDataSourceV2Exec.scala:186) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.AtomicReplaceTableAsSelectExec.run(WriteToDataSourceV2Exec.scala:221) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result$lzycompute(V2CommandExec.scala:43) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result(V2CommandExec.scala:43) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.executeCollect(V2CommandExec.scala:49) ~[spark-sql_2.12-3.5.5-amzn-0.jar:?]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:126) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.executeQuery$1(SQLExecution.scala:157) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$10(SQLExecution.scala:220) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$9(SQLExecution.scala:220) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:405) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:219) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:901) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:83) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:74) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:123) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:114) ~[spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:521) ~[spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:77) [spark-sql-api_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:521) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:34) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:303) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:299) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:497) [spark-catalyst_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:114) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:101) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:99) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:164) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.DataFrameWriterV2.runCommand(DataFrameWriterV2.scala:203) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.DataFrameWriterV2.internalReplace(DataFrameWriterV2.scala:215) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at org.apache.spark.sql.DataFrameWriterV2.createOrReplace(DataFrameWriterV2.scala:135) [spark-sql_2.12-3.5.5-amzn-0.jar:3.5.5-amzn-0]
	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:?]
	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[?:?]
	at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:?]
	at java.lang.reflect.Method.invoke(Method.java:569) ~[?:?]
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244) [py4j-0.10.9.7.jar:?]
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374) [py4j-0.10.9.7.jar:?]
	at py4j.Gateway.invoke(Gateway.java:282) [py4j-0.10.9.7.jar:?]
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132) [py4j-0.10.9.7.jar:?]
	at py4j.commands.CallCommand.execute(CallCommand.java:79) [py4j-0.10.9.7.jar:?]
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182) [py4j-0.10.9.7.jar:?]
	at py4j.ClientServerConnection.run(ClientServerConnection.java:106) [py4j-0.10.9.7.jar:?]
	at java.lang.Thread.run(Thread.java:840) [?:?]
25/09/05 18:45:56 INFO SQLExecution: Skipped SparkListenerSQLExecutionObfuscatedInfo event due to NON_EMPTY_ERROR.
25/09/05 18:45:56 INFO ExecutorContainerAllocator: Set total expected execs to {0=1}
25/09/05 18:45:56 INFO DefaultEmrServerlessRMClient: Containers created with container role SPARK_EXECUTOR. key to container id map: Map(111 -> 84cc8d99-8acc-b767-dd3d-7d2c56b99a94, 112 -> 06cc8d99-8acc-9d8c-5e90-268d297f0e45)
25/09/05 18:45:56 INFO SparkContext: Invoking stop() from shutdown hook
25/09/05 18:45:56 INFO SparkContext: SparkContext is stopping with exitCode 0.
25/09/05 18:45:56 INFO SparkUI: Stopped Spark web UI at http://[2600:1f12:dda:d200:24e6:bdae:866e:99f8]:4040
25/09/05 18:45:56 INFO EmrServerlessClusterSchedulerBackend: Shutting down all executors
25/09/05 18:45:56 INFO EmrServerlessClusterSchedulerBackend$EmrServerlessDriverEndpoint: Asking each executor to shut down
25/09/05 18:45:56 INFO ExecutorContainerAllocator: Releasing 6 executors at stop.
25/09/05 18:45:57 INFO TimeBasedRotatingEventLogFilesWriter: Renaming file:/var/log/spark/apps/eventlog_v2_00fvc7lmvahf2g03/00fvc7lmvahf2g03.inprogress to file:/var/log/spark/apps/eventlog_v2_00fvc7lmvahf2g03/events_1_00fvc7lmvahf2g03
25/09/05 18:45:57 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
25/09/05 18:45:57 INFO MemoryStore: MemoryStore cleared
25/09/05 18:45:57 INFO BlockManager: BlockManager stopped
25/09/05 18:45:57 INFO BlockManagerMaster: BlockManagerMaster stopped
25/09/05 18:45:57 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
25/09/05 18:45:57 INFO SparkContext: Successfully stopped SparkContext
25/09/05 18:45:57 INFO ShutdownHookManager: Shutdown hook called
25/09/05 18:45:57 INFO ShutdownHookManager: Deleting directory /tmp/spark-124ea3a3-cbd4-41bb-8822-292fe1567b68
25/09/05 18:45:57 INFO ShutdownHookManager: Deleting directory /tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1
25/09/05 18:45:57 INFO ShutdownHookManager: Deleting directory /tmp/spark-124ea3a3-cbd4-41bb-8822-292fe1567b68/pyspark-f561ccec-95ab-4d65-a58b-fb233897a824
























































[raw_dq_load] RANGE_FIELD=file_date RANGE_START=2020-04-22 RANGE_END=2020-04-29
DEBUG: Initializing ConfigurationManager with profile: None
DEBUG: Using profile from environment variable: gov_cloud.data_lake
DEBUG: Active profile set to: gov_cloud.data_lake
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
2025-09-05 18:29:54,671 mosaic.sparkCore.interface.spark_session_helper INFO     Setting warehouse location to: s3://entergy-govdatacore-raw-dev/
2025-09-05 18:30:26,059 root         INFO     Initializing RawDataCoreLoader
2025-09-05 18:30:26,059 root         INFO     App Name: eb_raw_ano_documents_file
2025-09-05 18:30:26,060 root         INFO     Job Type: raw
2025-09-05 18:30:26,060 root         INFO     Config Path: s3://entergy-govdatacore-dataeng-code-repo-dev/entergy-gov-data-core-code/config/eb/raw/ano_documents_file.yaml
2025-09-05 18:30:26,060 root         INFO     Getting credentials for secret: entergy_shared_b1EB01_EB_ANO
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
2025-09-05 18:30:26,176 root         INFO     Using force_schema=False for schema casting
DEBUG: ConfigurationManager already initialized with profile: gov_cloud.data_lake
DEBUG: Using existing ConfigurationManager instance
DEBUG: Using DynamoDB table name: govdatacore_etl_state
DEBUG: Active profile: {'account_ids': {'prod': '331875705534', 'test': '331876102067', 'dev': '331875467123'}, 'bucket_prefix': 'entergy-govdatacore', 'dynamodb': {'etl_state_table': 'govdatacore_etl_state', 'lock_table': 'govIcebergLockTable'}, 'storage': {'RAW': 's3://{bucket_prefix}-raw', 'DATA_QUALITY': 's3://{bucket_prefix}-dq', 'CURATED': 's3://{bucket_prefix}-curated', 'DATA_MART': 's3://{bucket_prefix}-datamart', 'FILES': 's3://{bucket_prefix}-raw-files', 'LANDING': 's3://{bucket_prefix}-landing', 'CODE': 's3://{bucket_prefix}-dataeng-code-repo'}, 'catalogs': {'production_id': '331875705534', 'production_name': 'shared_catalog', 'environment_name': 'glue_catalog'}, 'data_lake': True}
DEBUG: Using region: us-gov-west-1
2025-09-05 18:30:26,285 root         INFO     ETL state tracking from DynamoDB - target table: glue_catalog.eb_raw.ano_documents_file, field_type: timestamp
2025-09-05 18:30:26,285 root         INFO     Last run value from DynamoDB: 1900-01-01 00:00:00
2025-09-05 18:30:26,286 root         INFO     Load type: incremental
2025-09-05 18:30:26,286 root         INFO     Source table: dbo.Documents_File_View, Target table: glue_catalog.eb_raw.ano_documents_file
2025-09-05 18:30:26,286 root         INFO     Incremental field: file_date
2025-09-05 18:30:26,286 root         INFO     Field type for incremental load: timestamp
2025-09-05 18:30:26,286 root         INFO     Generated SQL query: (SELECT * FROM dbo.Documents_File_View WHERE file_date BETWEEN '1900-01-01 00:00:00' AND '2025-09-05 18:30:26') AS src
2025-09-05 18:30:26,286 root         INFO     Query details - load_type: incremental, incremental_field: file_date, last_run_value: 1900-01-01 00:00:00, field_type: timestamp
2025-09-05 18:30:26,286 root         INFO     Connecting to database: sqlserver on host: 148.194.70.31 port: 2000
2025-09-05 18:30:26,286 root         INFO     Database name/SID: EB_ANO
2025-09-05 18:30:26,286 root         INFO     Using JDBC URL: jdbc:sqlserver://148.194.70.31:2000;databaseName=EB_ANO;encrypt=true;trustServerCertificate=true
2025-09-05 18:30:26,286 root         INFO     Using JDBC driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
2025-09-05 18:31:33,633 root         INFO     Partition bounds for column document_id: min=102753, max=9799045
2025-09-05 18:31:33,635 root         INFO     Loading data from dbo.Documents_File_View to glue_catalog.eb_raw.ano_documents_file
2025-09-05 18:31:34,133 root         INFO     Generating pk_hash from fields: ['document_id']
2025-09-05 18:31:35,445 root         INFO     Iceberg table glue_catalog.eb_raw.ano_documents_file does not exist. Creating...
Traceback (most recent call last):
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/raw_dq_load.py", line 65, in <module>
    loader = RawDataCoreLoader(context=context)
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py", line 525, in __init__
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/transforms/base_loader.py", line 54, in __init__
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/io/raw_datacore_loader.py", line 708, in _load_table
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/transforms/base_loader.py", line 414, in _write_target_table
  File "/tmp/spark-f07c94f2-c23e-4c3c-a3e0-cd1174bfd9e1/mosaic.zip/mosaic/sparkCore/transforms/schema.py", line 195, in check_and_create_iceberg_table
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 2100, in createOrReplace
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 179, in deco
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o295.createOrReplace.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 1.0 failed 4 times, most recent failure: Lost task 3.3 in stage 1.0 (TID 32) ([2600:1f12:dda:d200:cd7a:9511:e174:68f2] executor 70): ExecutorLostFailure (executor 70 exited caused by one of the running tasks) Reason: Unknown executor exit code (137) (died from signal 9?)
Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:3083)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:3019)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:3018)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:3018)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1324)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1324)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1324)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3301)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3235)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:3224)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:1047)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2505)
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:390)
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2$(WriteToDataSourceV2Exec.scala:364)
	at org.apache.spark.sql.execution.datasources.v2.AppendDataExec.writeWithV2(WriteToDataSourceV2Exec.scala:230)
	at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run(WriteToDataSourceV2Exec.scala:342)
	at org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec.run$(WriteToDataSourceV2Exec.scala:341)
	at org.apache.spark.sql.execution.datasources.v2.AppendDataExec.run(WriteToDataSourceV2Exec.scala:230)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result$lzycompute(V2CommandExec.scala:43)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result(V2CommandExec.scala:43)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.executeCollect(V2CommandExec.scala:49)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:126)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108)
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384)
	at org.apache.spark.sql.execution.SQLExecution$.executeQuery$1(SQLExecution.scala:157)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$10(SQLExecution.scala:220)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108)
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$9(SQLExecution.scala:220)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:405)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:219)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:901)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:83)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:74)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:123)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:114)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:521)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:77)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:521)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:303)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:299)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:497)
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:114)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:101)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:99)
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:164)
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.$anonfun$writeToTable$1(WriteToDataSourceV2Exec.scala:582)
	at org.apache.spark.util.Utils$.tryWithSafeFinallyAndFailureCallbacks(Utils.scala:1410)
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.writeToTable(WriteToDataSourceV2Exec.scala:578)
	at org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec.writeToTable$(WriteToDataSourceV2Exec.scala:572)
	at org.apache.spark.sql.execution.datasources.v2.AtomicReplaceTableAsSelectExec.writeToTable(WriteToDataSourceV2Exec.scala:186)
	at org.apache.spark.sql.execution.datasources.v2.AtomicReplaceTableAsSelectExec.run(WriteToDataSourceV2Exec.scala:221)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result$lzycompute(V2CommandExec.scala:43)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.result(V2CommandExec.scala:43)
	at org.apache.spark.sql.execution.datasources.v2.V2CommandExec.executeCollect(V2CommandExec.scala:49)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:126)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108)
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384)
	at org.apache.spark.sql.execution.SQLExecution$.executeQuery$1(SQLExecution.scala:157)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$10(SQLExecution.scala:220)
	at org.apache.spark.sql.catalyst.QueryPlanningTracker$.withTracker(QueryPlanningTracker.scala:108)
	at org.apache.spark.sql.execution.SQLExecution$.withTracker(SQLExecution.scala:384)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$9(SQLExecution.scala:220)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:405)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:219)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:901)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:83)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:74)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:123)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:114)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:521)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:77)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:521)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:303)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:299)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:34)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:497)
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:114)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:101)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:99)
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:164)
	at org.apache.spark.sql.DataFrameWriterV2.runCommand(DataFrameWriterV2.scala:203)
	at org.apache.spark.sql.DataFrameWriterV2.internalReplace(DataFrameWriterV2.scala:215)
	at org.apache.spark.sql.DataFrameWriterV2.createOrReplace(DataFrameWriterV2.scala:135)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:569)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
	at java.base/java.lang.Thread.run(Thread.java:840)
