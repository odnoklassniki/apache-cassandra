-- In Log database:
CREATE TABLE CassandraClientOperation  ( 
    Counter    	bigint IDENTITY(1,1) NOT NULL,
    Registered 	smalldatetime NOT NULL,
    HostName   	varchar(32) NOT NULL,
    Group0     	varchar(64) NULL,
    Group1     	varchar(64) NULL,
    Group2     	varchar(64) NULL,
    Group3     	varchar(64) NULL,
    Group4     	varchar(64) NULL,
    Group5     	varchar(64) NULL,
    Calls      	int NOT NULL,
    DurationAvg	bigint NOT NULL,
    DurationMin	bigint NOT NULL,
    DurationMax	bigint NOT NULL,
    Failures   	int NOT NULL,
    Timeouts   	int NOT NULL 
    )
;
CREATE NONCLUSTERED INDEX IX_CassandraClientOperation_COUNTER
    ON CassandraClientOperation(Counter)
    ;
CREATE CLUSTERED INDEX IX_CassandraClientOperation_REGISTERED_
    ON CassandraClientOperation(Registered)
    ;

CREATE TABLE CassandraStat ( 
    Registered	smalldatetime NOT NULL,
    Counter   	bigint IDENTITY(1,1) NOT NULL,
    HostName  	varchar(32) NOT NULL,
    -- aggregator: stats.cassandra.server & stats.cassandra.client
    Group0    	varchar(64) NULL,
    -- operation: TP.ROW-READ, TP.ROW-MUTATION, LOAD, ROWCACHE-HIT, ROWCACHE-SIZE
    [Group1]        varchar(64) NULL,
    -- cassandra cluster name
    [Group2]        varchar(64) NULL,
    -- cassandra endpoint (server node)
    [Group3]        varchar(64) NULL,
    -- cassandra column family (ColumnFamily)
    [Group4]        varchar(64) NULL,
    [Value0]        int NOT NULL,
    [Value1]        int NULL
    )
;
CREATE INDEX IX_CassandraStat_Counter
    ON CassandraStat(Counter)
;
CREATE CLUSTERED INDEX IX_CassandraStat_Registered
    ON CassandraStat(Registered)
;
