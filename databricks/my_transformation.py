from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    from_json,
    concat,
    concat_ws,
    lit,
    current_timestamp,
    when,
    unix_timestamp,
    row_number,
    sum as spark_sum
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

from pyspark.sql.window import Window
from delta.tables import DeltaTable


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "0.tcp.in.ngrok.io:10037"
KAFKA_TOPIC = "users_created"


# ============================================================
# JSON SCHEMA
# ============================================================

user_schema = StructType([

    StructField("gender", StringType()),

    StructField(
        "name",
        StructType([
            StructField("title", StringType()),
            StructField("first", StringType()),
            StructField("last", StringType())
        ])
    ),

    StructField(
        "location",
        StructType([

            StructField(
                "street",
                StructType([
                    StructField("number", IntegerType()),
                    StructField("name", StringType())
                ])
            ),

            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("country", StringType()),
            StructField("postcode", StringType()),

            StructField(
                "coordinates",
                StructType([
                    StructField("latitude", StringType()),
                    StructField("longitude", StringType())
                ])
            ),

            StructField(
                "timezone",
                StructType([
                    StructField("offset", StringType()),
                    StructField("description", StringType())
                ])
            )
        ])
    ),

    StructField("email", StringType()),

    StructField(
        "login",
        StructType([
            StructField("uuid", StringType())
        ])
    ),

    StructField(
        "dob",
        StructType([
            StructField("date", StringType()),
            StructField("age", IntegerType())
        ])
    ),

    StructField(
        "registered",
        StructType([
            StructField("date", StringType()),
            StructField("age", IntegerType())
        ])
    ),

    StructField("phone", StringType()),
    StructField("cell", StringType()),

    StructField(
        "id",
        StructType([
            StructField("name", StringType()),
            StructField("value", StringType())
        ])
    ),

    StructField(
        "picture",
        StructType([
            StructField("large", StringType()),
            StructField("medium", StringType()),
            StructField("thumbnail", StringType())
        ])
    ),

    StructField("nat", StringType())
])


# ============================================================
# BRONZE
# ============================================================

@dp.table(
    name="users_bronze",
    comment="Raw Kafka events from users_created"
)
def users_bronze():

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
        .select(
            "key",
            "value",
            "topic",
            "partition",
            "offset",
            "timestamp",
            "timestampType"
        )
    )


# ============================================================
# SILVER
# ============================================================

@dp.table(
    name="users_silver",
    comment="Cleaned and structured customer data"
)
def users_silver():

    bronze = spark.readStream.table("users_bronze")

    parsed = (
        bronze
        .withColumn(
            "json_data",
            from_json(
                col("value").cast("string"),
                user_schema
            )
        )
        .filter(
            col("json_data").isNotNull()
            &
            col("json_data.login.uuid").isNotNull()
        )
    )

    return parsed.select(

        # ----------------------------------------------------
        # Customer identity
        # ----------------------------------------------------

        col("json_data.gender").alias("gender"),

        col("json_data.name.title").alias("title"),

        col("json_data.name.first").alias("first_name"),

        col("json_data.name.last").alias("last_name"),

        # ----------------------------------------------------
        # Address
        # ----------------------------------------------------

        concat(
            col("json_data.location.street.number").cast("string"),
            lit(", "),
            col("json_data.location.street.name")
        ).alias("street_info"),

        col("json_data.location.city").alias("city"),

        col("json_data.location.state").alias("state"),

        col("json_data.location.country").alias("country"),

        col("json_data.location.postcode").alias("postcode"),

        # ----------------------------------------------------
        # Coordinates
        # ----------------------------------------------------

        col("json_data.location.coordinates.latitude")
            .cast("double")
            .alias("latitude"),

        col("json_data.location.coordinates.longitude")
            .cast("double")
            .alias("longitude"),

        # ----------------------------------------------------
        # Timezone
        # ----------------------------------------------------

        col("json_data.location.timezone.offset")
            .alias("timezone_offset"),

        col("json_data.location.timezone.description")
            .alias("timezone_description"),

        # ----------------------------------------------------
        # Contact
        # ----------------------------------------------------

        col("json_data.email").alias("email"),

        # ----------------------------------------------------
        # User ID
        # ----------------------------------------------------

        col("json_data.login.uuid").alias("user_id"),

        # ----------------------------------------------------
        # DOB
        # ----------------------------------------------------

        col("json_data.dob.date").alias("birth_date"),

        col("json_data.dob.age").alias("age"),

        # ----------------------------------------------------
        # Registration
        # ----------------------------------------------------

        col("json_data.registered.date")
            .alias("registered_date"),

        col("json_data.registered.age")
            .alias("registered_age"),

        # ----------------------------------------------------
        # Phone
        # ----------------------------------------------------

        col("json_data.phone").alias("phone_number"),

        col("json_data.cell").alias("cell_number"),

        # ----------------------------------------------------
        # ID information
        # ----------------------------------------------------

        col("json_data.id.name").alias("id_name"),

        col("json_data.id.value").alias("id_value"),

        # ----------------------------------------------------
        # Pictures
        # ----------------------------------------------------

        col("json_data.picture.large")
            .alias("picture_large"),

        col("json_data.picture.medium")
            .alias("picture_medium"),

        col("json_data.picture.thumbnail")
            .alias("picture_thumbnail"),

        # ----------------------------------------------------
        # Nationality
        # ----------------------------------------------------

        col("json_data.nat").alias("nationality"),

        # ----------------------------------------------------
        # Pipeline metadata
        # ----------------------------------------------------

        col("timestamp")
            .alias("injection_timestamp"),

        current_timestamp()
            .alias("processing_timestamp")
    )


# ============================================================
# GOLD 1: CUSTOMERS
# ============================================================

@dp.materialized_view(
    name="gold_customers",
    comment="Current latest customer record per user"
)
def gold_customers():

    silver_df = spark.read.table("users_silver")

    # --------------------------------------------------------
    # Keep latest record for each user
    # --------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("user_id")
        .orderBy(
            col("injection_timestamp").desc(),
            col("processing_timestamp").desc()
        )
    )

    latest_df = (
        silver_df
        .withColumn(
            "rn",
            row_number().over(window_spec)
        )
        .filter(col("rn") == 1)
        .drop("rn")
    )

    return latest_df.select(

        col("user_id"),

        col("gender"),

        col("title"),

        col("first_name"),

        col("last_name"),

        concat_ws(
            " ",
            col("first_name"),
            col("last_name")
        ).alias("full_name"),

        col("email"),

        col("street_info"),

        col("city"),

        col("state"),

        col("country"),

        col("postcode"),

        col("latitude")
            .cast("double")
            .alias("latitude"),

        col("longitude")
            .cast("double")
            .alias("longitude"),

        col("timezone_offset"),

        col("timezone_description"),

        col("nationality"),

        col("age")
            .cast("int")
            .alias("age"),

        col("birth_date"),

        col("registered_date"),

        col("registered_age")
            .cast("int")
            .alias("registered_age"),

        col("phone_number"),

        col("cell_number"),

        col("id_name"),

        col("id_value"),

        col("picture_large"),

        col("picture_medium"),

        col("picture_thumbnail"),

        col("injection_timestamp"),

        col("processing_timestamp")
    )


# ============================================================
# GOLD 2: DATA QUALITY
# ============================================================

@dp.materialized_view(
    name="gold_data_quality",
    comment="Customer profile completeness and quality"
)
def gold_data_quality():

    customers_df = spark.read.table(
        "gold_customers"
    )

    quality_df = (
        customers_df

        .withColumn(
            "profile_completeness_score",

            (
                col("first_name").isNotNull().cast("int")
                +
                col("last_name").isNotNull().cast("int")
                +
                col("email").isNotNull().cast("int")
                +
                col("phone_number").isNotNull().cast("int")
                +
                col("country").isNotNull().cast("int")
                +
                col("city").isNotNull().cast("int")
                +
                col("age").isNotNull().cast("int")
            )
        )

        .withColumn(
            "profile_quality",

            when(
                col("profile_completeness_score") == 7,
                "Complete"
            )

            .when(
                col("profile_completeness_score") >= 5,
                "Mostly Complete"
            )

            .when(
                col("profile_completeness_score") >= 3,
                "Partially Complete"
            )

            .otherwise("Poor")
        )
    )

    return quality_df.select(

        "user_id",
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "country",
        "city",
        "age",
        "profile_completeness_score",
        "profile_quality",
        "injection_timestamp",
        "processing_timestamp"
    )


# ============================================================
# GOLD 3: PIPELINE METRICS
# ============================================================

@dp.materialized_view(
    name="gold_pipeline_metrics",
    comment="Pipeline processing latency metrics"
)
def gold_pipeline_metrics():

    customers_df = spark.read.table(
        "gold_customers"
    )

    metrics_df = (
        customers_df

        .withColumn(
            "processing_latency_seconds",

            when(
                (
                    unix_timestamp("processing_timestamp")
                    -
                    unix_timestamp("injection_timestamp")
                ) < 0,
                lit(0)
            ).otherwise(
                unix_timestamp("processing_timestamp")
                -
                unix_timestamp("injection_timestamp")
            )
        )
    )

    return metrics_df.select(

        "user_id",

        "injection_timestamp",

        "processing_timestamp",

        "processing_latency_seconds"
    )


# ============================================================
# GOLD 4: DEMOGRAPHIC STATE + DEMOGRAPHICS
# ============================================================

DEMOGRAPHIC_STATE_TABLE = (
    "data_engineering_project.kafka_pipeline.gold_demographic_state"
)

DEMOGRAPHICS_TABLE = (
    "data_engineering_project.kafka_pipeline.gold_demographics"
)


# ------------------------------------------------------------
# ForEachBatch sink
# ------------------------------------------------------------

@dp.foreach_batch_sink(
    name="demographics_state_sink"
)
def demographics_state_sink(batch_df, batch_id):

    spark_session = batch_df.sparkSession

    # --------------------------------------------------------
    # Ignore empty batches
    # --------------------------------------------------------

    if batch_df.isEmpty():
        return

    # --------------------------------------------------------
    # Create state table
    # --------------------------------------------------------

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {DEMOGRAPHIC_STATE_TABLE} (
            user_id STRING,
            gender STRING,
            age_group STRING,
            nationality STRING,
            injection_timestamp TIMESTAMP
        )
        USING DELTA
    """)

    # --------------------------------------------------------
    # Create demographics aggregation table
    # --------------------------------------------------------

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {DEMOGRAPHICS_TABLE} (
            gender STRING,
            age_group STRING,
            nationality STRING,
            customer_count BIGINT
        )
        USING DELTA
    """)

    # --------------------------------------------------------
    # Latest record per user in this micro-batch
    # --------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("user_id")
        .orderBy(
            col("injection_timestamp").desc(),
            col("processing_timestamp").desc()
        )
    )

    current_batch = (
        batch_df

        .withColumn(
            "rn",
            row_number().over(window_spec)
        )

        .filter(
            col("rn") == 1
        )

        .drop("rn")

        .withColumn(
            "age_group",

            when(
                col("age") < 18,
                "Under 18"
            )

            .when(
                col("age") <= 25,
                "18-25"
            )

            .when(
                col("age") <= 35,
                "26-35"
            )

            .when(
                col("age") <= 50,
                "36-50"
            )

            .otherwise("51+")
        )

        .select(
            "user_id",
            "gender",
            "age_group",
            "nationality",
            "injection_timestamp"
        )
    )

    # --------------------------------------------------------
    # Existing state
    # --------------------------------------------------------

    old_state = spark_session.table(
        DEMOGRAPHIC_STATE_TABLE
    )

    # --------------------------------------------------------
    # Only accept a record if:
    #
    # 1. User does not exist in state
    # OR
    # 2. Incoming record is newer
    # --------------------------------------------------------

    state_comparison = (
        current_batch.alias("new")

        .join(
            old_state.alias("old"),

            col("new.user_id")
            ==
            col("old.user_id"),

            "left"
        )

        .filter(
            col("old.user_id").isNull()
            |
            (
                col("new.injection_timestamp")
                >
                col("old.injection_timestamp")
            )
        )
    )

    # --------------------------------------------------------
    # Previous state
    # --------------------------------------------------------

    previous_state = (
        state_comparison

        .filter(
            col("old.user_id").isNotNull()
        )

        .select(
            col("old.user_id").alias("user_id"),
            col("old.gender").alias("gender"),
            col("old.age_group").alias("age_group"),
            col("old.nationality").alias("nationality")
        )
    )

    # --------------------------------------------------------
    # New state
    # --------------------------------------------------------

    new_state = (
        state_comparison

        .select(
            col("new.user_id").alias("user_id"),
            col("new.gender").alias("gender"),
            col("new.age_group").alias("age_group"),
            col("new.nationality").alias("nationality"),
            col("new.injection_timestamp")
                .alias("injection_timestamp")
        )
    )

    # --------------------------------------------------------
    # OLD STATE → -1
    # --------------------------------------------------------

    old_delta = (
        previous_state

        .withColumn(
            "delta",
            lit(-1).cast("long")
        )
    )

    # --------------------------------------------------------
    # NEW STATE → +1
    # --------------------------------------------------------

    new_delta = (
        new_state

        .select(
            "user_id",
            "gender",
            "age_group",
            "nationality"
        )

        .withColumn(
            "delta",
            lit(1).cast("long")
        )
    )

    # --------------------------------------------------------
    # Calculate incremental changes
    # --------------------------------------------------------

    deltas = (
        old_delta

        .unionByName(
            new_delta
        )

        .groupBy(
            "gender",
            "age_group",
            "nationality"
        )

        .agg(
            spark_sum("delta")
            .alias("delta")
        )

        .filter(
            col("delta") != 0
        )
    )

    # --------------------------------------------------------
    # UPDATE GOLD DEMOGRAPHICS
    # --------------------------------------------------------

    gold_table = DeltaTable.forName(
        spark_session,
        DEMOGRAPHICS_TABLE
    )

    (
        gold_table.alias("target")

        .merge(
            deltas.alias("source"),

            """
            target.gender = source.gender
            AND target.age_group = source.age_group
            AND target.nationality = source.nationality
            """
        )

        .whenMatchedUpdate(
            set={
                "customer_count":
                    "target.customer_count + source.delta"
            }
        )

        .whenNotMatchedInsert(
            values={
                "gender": "source.gender",
                "age_group": "source.age_group",
                "nationality": "source.nationality",
                "customer_count": "source.delta"
            }
        )

        .execute()
    )

    # --------------------------------------------------------
    # UPDATE DEMOGRAPHIC STATE
    # --------------------------------------------------------

    state_table = DeltaTable.forName(
        spark_session,
        DEMOGRAPHIC_STATE_TABLE
    )

    (
        state_table.alias("target")

        .merge(
            new_state.alias("source"),

            "target.user_id = source.user_id"
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )


# ------------------------------------------------------------
# Streaming source = SILVER
# ------------------------------------------------------------

@dp.append_flow(
    target="demographics_state_sink"
)
def demographics_flow():

    return spark.readStream.table(
        "users_silver"
    )


# ============================================================
# GOLD 5: GEOGRAPHY STATE + GEOGRAPHY
# ============================================================

GEOGRAPHY_STATE_TABLE = (
    "data_engineering_project.kafka_pipeline.gold_geography_state"
)

GEOGRAPHY_TABLE = (
    "data_engineering_project.kafka_pipeline.gold_geography"
)


# ------------------------------------------------------------
# ForEachBatch sink
# ------------------------------------------------------------

@dp.foreach_batch_sink(
    name="geography_state_sink"
)
def geography_state_sink(batch_df, batch_id):

    spark_session = batch_df.sparkSession

    # --------------------------------------------------------
    # Ignore empty batches
    # --------------------------------------------------------

    if batch_df.isEmpty():
        return

    # --------------------------------------------------------
    # Create geography state table
    # --------------------------------------------------------

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {GEOGRAPHY_STATE_TABLE} (
            user_id STRING,
            country STRING,
            state STRING,
            city STRING,
            injection_timestamp TIMESTAMP
        )
        USING DELTA
    """)

    # --------------------------------------------------------
    # Create geography aggregation table
    # --------------------------------------------------------

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {GEOGRAPHY_TABLE} (
            country STRING,
            state STRING,
            city STRING,
            customer_count BIGINT
        )
        USING DELTA
    """)

    # --------------------------------------------------------
    # Latest record per user in this micro-batch
    # --------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("user_id")
        .orderBy(
            col("injection_timestamp").desc(),
            col("processing_timestamp").desc()
        )
    )

    current_batch = (
        batch_df

        .withColumn(
            "rn",
            row_number().over(window_spec)
        )

        .filter(
            col("rn") == 1
        )

        .drop("rn")

        .select(
            "user_id",
            "country",
            "state",
            "city",
            "injection_timestamp"
        )
    )

    # --------------------------------------------------------
    # Existing geography state
    # --------------------------------------------------------

    old_state = spark_session.table(
        GEOGRAPHY_STATE_TABLE
    )

    # --------------------------------------------------------
    # Only accept a record if:
    #
    # 1. User does not exist in state
    # OR
    # 2. Incoming record is newer
    # --------------------------------------------------------

    state_comparison = (
        current_batch.alias("new")

        .join(
            old_state.alias("old"),

            col("new.user_id")
            ==
            col("old.user_id"),

            "left"
        )

        .filter(
            col("old.user_id").isNull()
            |
            (
                col("new.injection_timestamp")
                >
                col("old.injection_timestamp")
            )
        )
    )

    # --------------------------------------------------------
    # Previous geography state
    # --------------------------------------------------------

    previous_state = (
        state_comparison

        .filter(
            col("old.user_id").isNotNull()
        )

        .select(
            col("old.user_id").alias("user_id"),
            col("old.country").alias("country"),
            col("old.state").alias("state"),
            col("old.city").alias("city")
        )
    )

    # --------------------------------------------------------
    # New geography state
    # --------------------------------------------------------

    new_state = (
        state_comparison

        .select(
            col("new.user_id").alias("user_id"),
            col("new.country").alias("country"),
            col("new.state").alias("state"),
            col("new.city").alias("city"),
            col("new.injection_timestamp")
                .alias("injection_timestamp")
        )
    )

    # --------------------------------------------------------
    # OLD STATE → -1
    # --------------------------------------------------------

    old_delta = (
        previous_state

        .withColumn(
            "delta",
            lit(-1).cast("long")
        )
    )

    # --------------------------------------------------------
    # NEW STATE → +1
    # --------------------------------------------------------

    new_delta = (
        new_state

        .select(
            "user_id",
            "country",
            "state",
            "city"
        )

        .withColumn(
            "delta",
            lit(1).cast("long")
        )
    )

    # --------------------------------------------------------
    # Calculate incremental changes
    # --------------------------------------------------------

    deltas = (
        old_delta

        .unionByName(
            new_delta
        )

        .groupBy(
            "country",
            "state",
            "city"
        )

        .agg(
            spark_sum("delta")
            .alias("delta")
        )

        .filter(
            col("delta") != 0
        )
    )

    # --------------------------------------------------------
    # UPDATE GOLD GEOGRAPHY
    # --------------------------------------------------------

    gold_table = DeltaTable.forName(
        spark_session,
        GEOGRAPHY_TABLE
    )

    (
        gold_table.alias("target")

        .merge(
            deltas.alias("source"),

            """
            target.country = source.country
            AND target.state = source.state
            AND target.city = source.city
            """
        )

        .whenMatchedUpdate(
            set={
                "customer_count":
                    "target.customer_count + source.delta"
            }
        )

        .whenNotMatchedInsert(
            values={
                "country": "source.country",
                "state": "source.state",
                "city": "source.city",
                "customer_count": "source.delta"
            }
        )

        .execute()
    )

    # --------------------------------------------------------
    # UPDATE GEOGRAPHY STATE
    # --------------------------------------------------------

    state_table = DeltaTable.forName(
        spark_session,
        GEOGRAPHY_STATE_TABLE
    )

    (
        state_table.alias("target")

        .merge(
            new_state.alias("source"),

            "target.user_id = source.user_id"
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )


# ------------------------------------------------------------
# Streaming source = SILVER
# ------------------------------------------------------------

@dp.append_flow(
    target="geography_state_sink"
)
def geography_flow():

    return spark.readStream.table(
        "users_silver"
    )