"""
Azure Cloud Connectors

Connectors for Azure services including Blob Storage, Data Lake Gen2,
Synapse Analytics, Event Hubs, and Azure SQL Database.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class AzureConnector:
    """Base class for Azure connectors"""
    
    def __init__(
        self,
        spark: SparkSession,
        account_name: str,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.account_name = account_name
        self.config = config or {}
    
    def _configure_auth(self, auth_type: str = "account_key"):
        """Configure Azure authentication"""
        if auth_type == "account_key" and "account_key" in self.config:
            self.spark.conf.set(
                f"fs.azure.account.key.{self.account_name}.dfs.core.windows.net",
                self.config["account_key"]
            )
        elif auth_type == "sas_token" and "sas_token" in self.config:
            self.spark.conf.set(
                f"fs.azure.sas.{self.config['container']}.{self.account_name}.blob.core.windows.net",
                self.config["sas_token"]
            )
        elif auth_type == "managed_identity":
            self.spark.conf.set(
                "fs.azure.account.auth.type",
                "OAuth"
            )
            self.spark.conf.set(
                "fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
            )


class AzureBlobConnector(AzureConnector):
    """
    Connector for Azure Blob Storage
    
    Example:
        >>> connector = AzureBlobConnector(
        ...     spark=spark,
        ...     account_name="mystorageaccount",
        ...     container="raw-data",
        ...     config={"account_key": "..."}
        ... )
        >>> df = connector.read(path="input/sales.parquet", format="parquet")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        account_name: str,
        container: str,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(spark, account_name, config)
        self.container = container
        self._configure_auth(config.get("auth_type", "account_key") if config else "account_key")


    def _find_index_for_key(key: str) -> Optional[int]:
        # сначала точное совпадение имени
        if key in name_to_idx:
            return name_to_idx[key]
        # затем по подстроке в имени или роли
        key_l = key.lower()
        for i, n in enumerate(names):
            if key_l in n.lower():
                return i
        for i, r in enumerate(roles):
            if key_l in r.lower():
                return i
        return None

    goal_theta = np.full((D,), np.nan, dtype=np.float64)

    for g in goals_list:
        for chan_key, target in getattr(g, "channel_targets", {}).items():
            idx = _find_index_for_key(str(chan_key))
            if idx is None or not (0 <= idx < D):
                continue

            # target может быть:
            # - int (дискретный класс → равномерное квантование 2π * id / M)
            # - float (фаза в радианах)
            # - np.ndarray (берём скаляр, если shape==())
            t_val: float
            if isinstance(target, np.ndarray):
                if target.ndim == 0:
                    t_val = float(target)
                else:
                    # Берём первый элемент как эвристику
                    t_val = float(target.ravel()[0])
            elif isinstance(target, (int, float)):
                t_val = float(target)
            else:
                # Неподдерживаемый тип цели — пропускаем
                continue

            # Если целое и у нас есть M для канала → квантование на окружности.
            M_ch = None
            try:
                if hasattr(schema, "channels") and schema.channels is not None:
                    M_ch = int(getattr(schema.channels[idx], "M", 0))  # type: ignore[index]
            except Exception:  # noqa: BLE001
                M_ch = None

            if M_ch is not None and M_ch > 0 and float(t_val).is_integer():
                # дискретный класс id → равномерная фаза
                goal_theta[idx] = 2.0 * np.pi * (int(t_val) % M_ch) / float(M_ch)
            else:
                # считаем, что t_val уже в радианах
                goal_theta[idx] = float(t_val)

    return goal_theta

    
    def read(
        self,
        path: str,
        format: str = "parquet",
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from Azure Blob Storage
        
        Args:
            path: Path within the container
            format: File format (parquet, csv, json, delta)
            options: Additional Spark read options
            
        Returns:
            Spark DataFrame
        """
        full_path = f"wasbs://{self.container}@{self.account_name}.blob.core.windows.net/{path}"
        
        logger.info(f"Reading from Azure Blob: {full_path}")
        
        reader = self.spark.read.format(format)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load(full_path)
    
    def write(
        self,
        df: DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to Azure Blob Storage
        
        Args:
            df: Spark DataFrame to write
            path: Target path within the container
            format: File format (parquet, csv, json, delta)
            mode: Write mode (overwrite, append, ignore, error)
            options: Additional Spark write options
        """
        full_path = f"wasbs://{self.container}@{self.account_name}.blob.core.windows.net/{path}"
        
        logger.info(f"Writing to Azure Blob: {full_path}")
        
        writer = df.write.format(format).mode(mode)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save(full_path)


class AzureDataLakeGen2Connector(AzureConnector):
    """
    Connector for Azure Data Lake Storage Gen2
    
    Example:
        >>> connector = AzureDataLakeGen2Connector(
        ...     spark=spark,
        ...     account_name="mydatalake",
        ...     container="bronze",
        ...     config={"account_key": "..."}
        ... )
        >>> df = connector.read(path="raw/events/*.parquet")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        account_name: str,
        container: str,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(spark, account_name, config)
        self.container = container
        self._configure_auth(config.get("auth_type", "account_key") if config else "account_key")
    
    def read(
        self,
        path: str,
        format: str = "parquet",
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from Azure Data Lake Gen2 using ABFS protocol
        
        Args:
            path: Path within the container
            format: File format (parquet, csv, json, delta)
            options: Additional Spark read options
            
        Returns:
            Spark DataFrame
        """
        full_path = f"abfss://{self.container}@{self.account_name}.dfs.core.windows.net/{path}"
        
        logger.info(f"Reading from ADLS Gen2: {full_path}")
        
        reader = self.spark.read.format(format)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load(full_path)
    
    def write(
        self,
        df: DataFrame,
        path: str,
        format: str = "parquet",
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to Azure Data Lake Gen2
        
        Args:
            df: Spark DataFrame to write
            path: Target path within the container
            format: File format (parquet, csv, json, delta)
            mode: Write mode (overwrite, append, ignore, error)
            partition_by: List of columns to partition by
            options: Additional Spark write options
        """
        full_path = f"abfss://{self.container}@{self.account_name}.dfs.core.windows.net/{path}"
        
        logger.info(f"Writing to ADLS Gen2: {full_path}")
        
        writer = df.write.format(format).mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save(full_path)


class AzureSynapseConnector:
    """
    Connector for Azure Synapse Analytics
    
    Example:
        >>> connector = AzureSynapseConnector(
        ...     spark=spark,
        ...     server="mysynapse.sql.azuresynapse.net",
        ...     database="sales_dw",
        ...     config={"user": "admin", "password": "..."}
        ... )
        >>> df = connector.read(table="fact_sales")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        server: str,
        database: str,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.server = server
        self.database = database
        self.config = config or {}
        
        # Configure connection properties
        self.jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database}"
        self.connection_properties = {
            "user": config.get("user", ""),
            "password": config.get("password", ""),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
    
    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read data from Synapse Analytics
        
        Args:
            table: Table name to read (use table or query, not both)
            query: SQL query to execute (use table or query, not both)
            options: Additional JDBC options
            
        Returns:
            Spark DataFrame
        """
        if table and query:
            raise ValueError("Provide either table or query, not both")
        
        if not table and not query:
            raise ValueError("Provide either table or query")
        
        logger.info(f"Reading from Synapse: {table or 'custom query'}")
        
        reader = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url)
        
        for key, value in self.connection_properties.items():
            reader = reader.option(key, value)
        
        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()
    
    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None
    ):
        """
        Write data to Synapse Analytics
        
        Args:
            df: Spark DataFrame to write
            table: Target table name
            mode: Write mode (overwrite, append, ignore, error)
            options: Additional JDBC options
        """
        logger.info(f"Writing to Synapse table: {table}")
        
        writer = df.write.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table) \
            .mode(mode)
        
        for key, value in self.connection_properties.items():
            writer = writer.option(key, value)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save()


class AzureEventHubConnector:
    """
    Connector for Azure Event Hubs (streaming)
    
    Example:
        >>> connector = AzureEventHubConnector(
        ...     spark=spark,
        ...     namespace="myeventhub",
        ...     eventhub_name="events",
        ...     config={"connection_string": "..."}
        ... )
        >>> df = connector.read_stream()
    """
    
    def __init__(
        self,
        spark: SparkSession,
        namespace: str,
        eventhub_name: str,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.namespace = namespace
        self.eventhub_name = eventhub_name
        self.config = config or {}
        
        # Build connection string
        if "connection_string" in config:
            self.connection_string = config["connection_string"]
        else:
            raise ValueError("connection_string is required in config")
    
    def read_stream(
        self,
        starting_position: str = "earliest",
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read streaming data from Event Hub
        
        Args:
            starting_position: Starting position (earliest, latest, or timestamp)
            options: Additional Event Hub options
            
        Returns:
            Streaming DataFrame
        """
        logger.info(f"Reading stream from Event Hub: {self.eventhub_name}")
        
        eh_conf = {
            "eventhubs.connectionString": self.connection_string,
            "eventhubs.startingPosition": f'{{"offset": "{starting_position}"}}'
        }
        
        if options:
            eh_conf.update(options)
        
        return self.spark.readStream \
            .format("eventhubs") \
            .options(**eh_conf) \
            .load()
    
    def write_stream(
        self,
        df: DataFrame,
        checkpoint_location: str,
        output_mode: str = "append",
        trigger: str = "processingTime='10 seconds'"
    ):
        """
        Write streaming data to Event Hub
        
        Args:
            df: Streaming DataFrame to write
            checkpoint_location: Checkpoint location for fault tolerance
            output_mode: Output mode (append, update, complete)
            trigger: Trigger interval
        """
        logger.info(f"Writing stream to Event Hub: {self.eventhub_name}")
        
        eh_conf = {
            "eventhubs.connectionString": self.connection_string
        }
        
        query = df.writeStream \
            .format("eventhubs") \
            .outputMode(output_mode) \
            .options(**eh_conf) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=trigger) \
            .start()
        
        return query


class AzureSQLConnector:
    """
    Connector for Azure SQL Database
    
    Example:
        >>> connector = AzureSQLConnector(
        ...     spark=spark,
        ...     server="myserver.database.windows.net",
        ...     database="salesdb",
        ...     config={"user": "admin", "password": "..."}
        ... )
        >>> df = connector.read(table="customers")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        server: str,
        database: str,
        config: Optional[Dict[str, Any]] = None
    ):
        self.spark = spark
        self.server = server
        self.database = database
        self.config = config or {}
        
        # Configure connection
        self.jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"
        self.connection_properties = {
            "user": config.get("user", ""),
            "password": config.get("password", ""),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
    
    def read(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """Read data from Azure SQL Database"""
        if table and query:
            raise ValueError("Provide either table or query, not both")
        
        logger.info(f"Reading from Azure SQL: {table or 'custom query'}")
        
        reader = self.spark.read.format("jdbc") \
            .option("url", self.jdbc_url)
        
        for key, value in self.connection_properties.items():
            reader = reader.option(key, value)
        
        if table:
            reader = reader.option("dbtable", table)
        else:
            reader = reader.option("query", query)
        
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        return reader.load()
    
    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, str]] = None
    ):
        """Write data to Azure SQL Database"""
        logger.info(f"Writing to Azure SQL table: {table}")
        
        writer = df.write.format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table) \
            .mode(mode)
        
        for key, value in self.connection_properties.items():
            writer = writer.option(key, value)
        
        if options:
            for key, value in options.items():
                writer = writer.option(key, value)
        
        writer.save()


# Export all connectors
__all__ = [
    "AzureConnector",
    "AzureBlobConnector",
    "AzureDataLakeGen2Connector",
    "AzureSynapseConnector",
    "AzureEventHubConnector",
    "AzureSQLConnector"
]
