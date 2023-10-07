package com.henry.ossrepro.postgresql.integratedtests;

import static org.mockito.ArgumentMatchers.any;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.henry.ossrepro.TraceIdHelper;
import com.henry.ossrepro.TrackingRecord;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@ExtendWith(MockitoExtension.class)
@DirtiesContext
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class PostgresqlSerializableInsertWhereNotExistsIntegratedTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PostgresqlSerializableInsertWhereNotExistsIntegratedTest.class);

  private static final String PARENT_REQUEST_TRACE_ID_DEVX =
      "devx-quality-9e6eb7fc-41f3-4596-970b-b00393b12ea2";
  private static final String MIDDLE_TRACE_ID_DEVX =
      PARENT_REQUEST_TRACE_ID_DEVX + "_764aaf71-bfe1-4a0e-a7c0-711a62ad732c";
  private static final String CHUNKED_TRACE_ID_DEVX = MIDDLE_TRACE_ID_DEVX + "-part-0";
  private static final String VENDOR_ID = "fb81bcba-92fc-496c-a789-b4b5af4019f1";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final String AND_VENDOR_ID_SQL = " and vendor_id = ? ";

  private static final String INSERT_STATUS_SQL =
      "insert into status (trace_id, parent_trace_id, entity_name, entity_operation, entity_version, vendor_id, status, updated_at, created_at, trace_id_failed_chunks) select ?, ?, ?, ?, ?, ?, ?, ?, ?, ? where \n"
          // last: trace_id_failed_chunks
          + " not exists (\n"
          + " select 1 from status where "
          + " trace_id = ? " // traceId
          + AND_VENDOR_ID_SQL // vendorId
          + " and created_at >= ? \n" // createdAt calculation
          + " limit 1 "
          + " ) ";

  @SpyBean
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private PlatformTransactionManager platformTransactionManager;

  private static final ExecutorService asyncExecutorService = Executors.newFixedThreadPool(128);

  @SuppressWarnings("resource") // TestContainers managed resource lifecycle
  @Container
  static final PostgreSQLContainer<?> postgreSQLContainer =
      new PostgreSQLContainer<>(
          DockerImageName.parse("timescale/timescaledb-ha:pg15-all")
              .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("data_ingestion")
          .withUsername("admin")
          .withPassword("admin")
          .withInitScript("postgres-scripts/ddl-00.sql");

  private TransactionTemplate transactionTemplate;
  private RetryTemplate retryTemplate;

  @DynamicPropertySource
  static void overrideConfiguration(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.datasource.url",
        () ->
            "jdbc:postgresql://localhost:"
                + postgreSQLContainer.getMappedPort(5432)
                + "/data_ingestion?autosave=always&cleanupSavepoints=true");
    registry.add("spring.datasource.username", postgreSQLContainer::getUsername);
    registry.add("spring.datasource.password", postgreSQLContainer::getPassword);
    registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
  }

  @BeforeEach
  void beforeEach() {
    this.transactionTemplate = new TransactionTemplate(platformTransactionManager);
    this.transactionTemplate.setIsolationLevel(TransactionTemplate.ISOLATION_SERIALIZABLE);
    this.transactionTemplate.setTimeout(20);
    this.transactionTemplate.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
    this.retryTemplate = RetryTemplate.builder()
        .maxAttempts(64)
        .exponentialBackoff(10, 2, 500, true)
        .traversingCauses()
        .retryOn(ConcurrencyFailureException.class)
        .build();
  }

  @AfterEach
  void afterEach() throws JsonProcessingException {
    List<Map<String, Object>> statusResults =
        jdbcTemplate.query(
            "SELECT * FROM status",
            PostgresqlSerializableInsertWhereNotExistsIntegratedTest::resultSetToMap);
    LOGGER.info("'status' table results: {}", objectMapper.writeValueAsString(statusResults));
    jdbcTemplate.update("TRUNCATE TABLE status");
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @AfterAll
  static void afterAll() throws InterruptedException {
    asyncExecutorService.shutdown();
    boolean terminated = asyncExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    if (!terminated) {
      asyncExecutorService.shutdownNow();
      asyncExecutorService.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  static Stream<Arguments> skewParameters() {
    return Stream.of(
        Arguments.arguments(Duration.ZERO),
        Arguments.arguments(Duration.ofMinutes(1)),
        Arguments.arguments(Duration.ofMinutes(5)),
        Arguments.arguments(Duration.ofMinutes(15)),
        Arguments.arguments(Duration.ofMinutes(30)));
  }

  static Stream<Arguments> skewAndSeedDelayParameters() {
    return skewParameters()
        .flatMap(
            skewArgs ->
                seedDelayParameters()
                    .map(delayArgs -> Arguments.arguments(skewArgs.get()[0], delayArgs.get()[0])));
  }

  static Stream<Arguments> skewAndSeedDelayAndBooleanParameters() {
    return skewAndSeedDelayParameters()
        .flatMap(
            skewAndSeedDelayArgs ->
                Stream.of(
                    Arguments.arguments(skewAndSeedDelayArgs.get()[0],
                        skewAndSeedDelayArgs.get()[1], false),
                    Arguments.arguments(skewAndSeedDelayArgs.get()[0],
                        skewAndSeedDelayArgs.get()[1], true)
                )
        );
  }

  static Stream<Arguments> seedDelayParameters() {
    return IntStream.range(0, 5).mapToObj(Arguments::arguments);
  }

  @Timeout(15)
  @ParameterizedTest
  @MethodSource("skewAndSeedDelayAndBooleanParameters")
  void testAsyncQueryConcurrentWrites(Duration durationSkew, long randomSeed,
      boolean isSynchronized) throws Throwable {
    spyDelayWithRandomSeed(randomSeed);
    createAndJoinOuterTasksForTest(
        taskNumber -> {
          String dbPrefix = buildDurationAndRandDbPrefix(durationSkew, randomSeed, taskNumber);
          Thread.currentThread().setName("TT-" + dbPrefix);
          TrackingRecord relayRecord =
              trackingRecordBuilderWithDefaults()
                  .setSourceSystem("GENERIC_RELAY")
                  .setCreatedAt(1695032264_590L)
                  .setParentTraceId(dbPrefix + PARENT_REQUEST_TRACE_ID_DEVX)
                  .setTraceId(dbPrefix + MIDDLE_TRACE_ID_DEVX)
                  .setSuccess(true)
                  .setDurationMs(1)
                  .setSteps(List.of("CHUNK_TYPE-INDIVIDUAL_LIST"))
                  .build();
          TrackingRecord vsFailedRecord =
              trackingRecordBuilderWithDefaults()
                  .setSourceSystem("ORDERS")
                  .setCreatedAt(1695032264_601L + durationSkew.toMillis())
                  .setParentTraceId(dbPrefix + MIDDLE_TRACE_ID_DEVX)
                  .setTraceId(dbPrefix + CHUNKED_TRACE_ID_DEVX)
                  .setId(UUID.randomUUID().toString())
                  .setSuccess(false)
                  .setErrorMessage("Order VS failed!")
                  .setDurationMs(39)
                  .build();
          TrackingRecord etlRecordPast =
              trackingRecordBuilderWithDefaults()
                  .setSourceSystem("ETL")
                  .setCreatedAt(1695032264_593L + durationSkew.toMillis())
                  .setParentTraceId(dbPrefix + MIDDLE_TRACE_ID_DEVX)
                  .setTraceId(dbPrefix + CHUNKED_TRACE_ID_DEVX)
                  .setSuccess(true)
                  .setDurationMs(16)
                  .build();
          TrackingRecord etlRecordFuture =
              trackingRecordBuilderWithDefaults()
                  .setSourceSystem("ETL")
                  .setCreatedAt(1695032264_801L + durationSkew.toMillis() * 2)
                  .setParentTraceId(dbPrefix + MIDDLE_TRACE_ID_DEVX)
                  .setTraceId(dbPrefix + CHUNKED_TRACE_ID_DEVX)
                  .setSuccess(true)
                  .setDurationMs(32)
                  .build();
          CompletableFuture<?>[] innerTasks =
              new CompletableFuture<?>[]{
                  insertWhereNotExists("TIT-relay-", dbPrefix, relayRecord, isSynchronized),
                  insertWhereNotExists("TIT-vs-", dbPrefix, vsFailedRecord, isSynchronized),
                  insertWhereNotExists("TIT-etl-past-", dbPrefix, etlRecordPast, isSynchronized),
                  insertWhereNotExists("TIT-etl-future-", dbPrefix, etlRecordFuture,
                      isSynchronized),
              };
          CompletableFuture.allOf(innerTasks).join();
          assertSingleStatusExistsInDatabase(dbPrefix + MIDDLE_TRACE_ID_DEVX);
        });
  }

  private boolean insertStatus(String traceIdWithChunk, String parentTraceId, String status,
      String vendorId, long createdAt, boolean isSynchronized) {
    if (!isSynchronized) {
      return doInsertStatus(traceIdWithChunk, parentTraceId, status, vendorId, createdAt);
    }
    synchronized (this) {
      return doInsertStatus(traceIdWithChunk, parentTraceId, status, vendorId, createdAt);
    }
  }

  private boolean doInsertStatus(String traceIdWithChunk, String parentTraceId, String status,
      String vendorId, long createdAt) {
    String traceIdWithoutChunk = TraceIdHelper.getTraceIdFromChunk(traceIdWithChunk);
    createdAt = adjustCreatedAt(createdAt);
    try {
      List<String> chunkList =
          status.contains("FAIL") ? List.of(traceIdWithChunk) : Collections.emptyList();
      Array chunkSQLArray = getSQLArray(chunkList);
      Timestamp timestampSearch = new Timestamp(createdAt - Duration.ofHours(6).toMillis());
      long updatedAt = Instant.now().toEpochMilli();
      int affected = jdbcTemplate.update(
          INSERT_STATUS_SQL,
          StringUtils.trim(traceIdWithoutChunk),
          StringUtils.trim(parentTraceId),
          StringUtils.trim("ENTITY_NAME"),
          StringUtils.trim("ENTITY_OPERATION"),
          StringUtils.trim("ENTITY_VERSION"),
          StringUtils.trim(vendorId),
          status,
          new Timestamp(updatedAt),
          new Timestamp(createdAt),
          chunkSQLArray,
          StringUtils.trim(traceIdWithoutChunk),
          StringUtils.trim(vendorId),
          timestampSearch
      );
      if (affected > 0) {
        return true;
      }
      throw new DuplicateKeyException("Matched status already exists, matched virtually by query!");
    } catch (DuplicateKeyException e) {
      LOGGER.debug("Duplicate status detected!", e);
      return false;
    }
  }

  private static long adjustCreatedAt(long createdAt) {
    return Instant.ofEpochMilli(createdAt).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
  }

  private CompletableFuture<?> insertWhereNotExists(String tag, String dbPrefix,
      TrackingRecord trackingRecord, boolean isSynchronized) {
    return CompletableFuture.runAsync(
        () -> {
          Thread.currentThread().setName(tag + dbPrefix);
          retryTemplate.execute(
              retryData -> transactionTemplate.execute(
                  transactionData -> insertStatus(
                      trackingRecord.getTraceId(),
                      trackingRecord.getParentTraceId(),
                      trackingRecord.getSuccess() ? "SUCCESS" : "FAILURE",
                      trackingRecord.getVendorId(),
                      trackingRecord.getCreatedAt(),
                      isSynchronized
                  )
              )
          );
        }
        , asyncExecutorService
    );
  }

  Array getSQLArray(List<String> stringList) {
    try {
      return jdbcTemplate.execute(
          (Connection connection) ->
              connection.createArrayOf(
                  "TEXT", stringList.stream().map(StringUtils::trim).toArray()));
    } catch (DataAccessException e) {
      throw new RuntimeException("Failed to create array! List: " + stringList, e);
    }
  }

  private static TrackingRecord.Builder trackingRecordBuilderWithDefaults() {
    return TrackingRecord.newBuilder()
        .setOperation("POST")
        .setEntity("ORDERS")
        .setVersion("V3")
        .setVendorId(VENDOR_ID)
        .setCountry("EC")
        .setSteps(Collections.emptyList())
        .setId(UUID.randomUUID().toString())
        .setDurationMs(1);
  }

  private void assertSingleStatusExistsInDatabase(String traceId) {
    Integer statusCount =
        jdbcTemplate
            .query(
                "SELECT COUNT(1) as counted FROM status WHERE trace_id = ? AND vendor_id = ?",
                (resultSet, rowNum) -> resultSet.getInt("counted"),
                traceId,
                VENDOR_ID)
            .get(0);
    Assertions.assertEquals(
        1, statusCount, "More or less than one status has been created for traceId " + traceId);
  }

  @NotNull
  private static Map<String, Object> resultSetToMap(ResultSet resultSet, int rowNum)
      throws SQLException {
    Map<String, Object> statusMap = new HashMap<>();
    ResultSetMetaData metadata = resultSet.getMetaData();
    int columnCount = metadata.getColumnCount();
    for (int i = 1; i <= columnCount; i++) {
      String columnName = metadata.getColumnName(i);
      Object obj = resultSet.getObject(columnName);
      if (obj instanceof Array) {
        obj = ((Array) obj).getArray();
      }
      statusMap.put(columnName, obj);
    }
    return statusMap;
  }

  @NotNull
  private static String buildDurationAndRandDbPrefix(
      Duration durationSkew, long randomSeed, Integer i) {
    return "dur{" + durationSkew.toString() + "}" + buildRandDbPrefix(randomSeed, i);
  }

  void createAndJoinOuterTasksForTest(Consumer<Integer> task) throws Throwable {
    CompletableFuture<?>[] outerTasks =
        IntStream.range(0, 32)
            .mapToObj(
                iterationNumber ->
                    CompletableFuture.runAsync(
                        () -> task.accept(iterationNumber), asyncExecutorService))
            .toArray(CompletableFuture[]::new);
    try {
      CompletableFuture.allOf(outerTasks).join();
    } catch (CompletionException e) {
      throw e.getCause();
    }
  }

  @NotNull
  private static String buildRandDbPrefix(long randomSeed, Integer i) {
    return "rand-" + randomSeed + "-iter-" + i + "-";
  }

  @SuppressWarnings("squid:S2925") // Used for concurrency fuzzing, not waiting for results
  private void spyDelayWithRandomSeed(long randomSeed) {
    Random random = new Random(randomSeed);
    Mockito.doAnswer(
            invocationOnMock -> {
              int sleepMs = random.nextInt(250);
              Thread.sleep(sleepMs);
              return invocationOnMock.callRealMethod();
            })
        .when(jdbcTemplate)
        .update(any(), any(Object[].class));
  }

}
