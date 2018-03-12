# nortrom
Flume JDBC sink using flume event's header and body to render insertion SQL.

## Sample

1. Given flume event below.
    ```json
    {
      "header": {
        "field2": "value2"
      },
      "body": {
        "field1": "value1"
      }
    }
    ```

2. flume's configuration  
Connect to MySQL `jdbc:mysql://mysql-address/test`, extract the Flume event's body
as JSON, and construct an insertion SQL `INSERT INTO test(field1, field2) VALUES ('value1', 'value2')`
    ```properties
    agent1.sinks.k1.type = com.wallstreetcn.flume.JDBCSink
    agent1.sinks.k1.driver = com.mysql.jdbc.Driver
    agent1.sinks.k1.connectionURL = jdbc:mysql://mysql-address/test
    agent1.sinks.k1.mapping = field1:body.field1,field2:header.field2
    agent1.sinks.k1.user = root
    agent1.sinks.k1.password = root
    agent1.sinks.k1.table = test
    agent1.sinks.k1.sqlDialect = MySQL
    ```
