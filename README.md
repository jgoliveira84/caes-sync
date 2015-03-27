# Caes-Sync
Simple daemon to sync data between ElasticSearch and Cassandra.

## Installing

Download this repository, extract it, go to its root and do:
```shell 
$ python setup.py install
```

This will install Caes-Sync and all its dependencies.

## Running

We assume that you have already setup both an [ElasticSearch](https://www.elastic.co/ "Elastic Search") instance and a [Cassandra](http://cassandra.apache.org/ "Cassandra") one. Once Caes-Sync is instaled, you need an [Yaml](http://yaml.org) config file at ~/.caes/config.yaml where you are going to put ElasticSearch/Cassandra conection parameters and other caes-sync stuff. This file's semantics is described in the *Configuring* section.

To start Caes-Sync do:

```shell
caes-sync-daemon start
```

and to stop it:

```shell
caes-sync-daemon stop
```

Not surprisingly

```shell
caes-sync-daemon restart
```

will restart the daemon.

## Schema

To make data syncing between heterogeneous technologies such as ElasticSearch and Cassandra possible, you need to conform your data to certain guidelines, mainly due to performance and/or consistency issues.

##### ElasticSearch 'schema'

ElasticSearch already has a 'built-in' field named *_timestamp* where the client application can provide a unix timestamp to associate with a document and make queries based on it. However, this field is not 'turned on' by default and you need, therefore, to have this mapping put on your index:

```javascript
{<type>: 
    {'_timestamp': 
      {
        'enabled': True, 
        'store': True
      }
    }
 }
```

This makes it possible for caes-sync to make fast range queries and get docs that where inserted/updated since the last sync.

We also need to garantue consistency across both 'databases'. For this we will rely on an optimistyc consistency control approach and use ElasticSearch's 'built-in' field *_version* along with *_version_type=external*. When indexing a document, you need to provide **the same value to _timestamp and _version**. So, your inserts/updates should look something like that:

```shell
$ curl -XPUT 'http://localhost:9200/<index>/<type>/ed31ee76-d49a-4c05-8f0a-6a856a94de8e?timestamp=1427254212&version=1427254212&version_type=external' -d '{
    "user" : "jgoliveira84",
    "message" : "Let's sync!!"
}'
```

##### Cassandra 'schema'

In Cassandra, what we need to do is provide a table that will act like the range query index, and also garantue consistency, since its entries will allways be ordered by the client-provided timestamp. So, we need to define a table like this 

```SQL
CREATE TABLE <timeseries_column_family> (
  <timeseries_id_fiel_dname> int,
  timestamp int,
  <data_id_field_name> uuid,
  PRIMARY KEY(<timeseries_id_field_name>, timestamp)
);
```
The field *data_id_fieldname* will reference another table, which in turn will have the actual data. This table can have the any fields you want, but keep in mind that you should make shure that all fields eventually comming from ElasticSearch have their counterparts on the *data_column_family*. That table needs to have a UUID type 4 Primary Key, and may look like

```SQL
CREATE TABLE <timeseries_column_family> (
  <data_id_field_name> uuid,
  vint int,
  vstring text,
  PRIMARY KEY(<data_id_field_name>)
);
```

When doing an insert/update you need to make it on both tables in a atomic way. So, your writes in Cassandra will look like

```SQL
BEGIN BATCH
    INSERT INTO <timeseries_column_family> (<timeseries_id_field_name>, <timeseries_field_name>, <data_id_field_name>) VALUES (?, ?, ?)
    INSERT INTO <data_column_family> (<data_id_field_name>, vstring, vint) VALUES (?, ?, ?)
APPLY BATCH;
```

##Configuring

You configure your Caes-Sync daemon by providing an [Yaml](http://yaml.org) file at ~/.caes/config.yaml. Here is an example of this file:

```Yaml
interval: 60

ElasticSearchConfig:
    index: data3
    type: tweet

CassandraConfig:
    keyspace: demo
    dataColumnFamily: data2
    timeseriesColumnFamily: ts2
    dataIdFieldName: did
    timestampFieldName: timestamp

logging:
  version: 1

  formatters:
      simple:
        format:  '%(asctime)s %(levelname)s %(name)s %(message)s'
        datefmt: '%Y/%m/%d %H:%M:%S'

  handlers:
      console:
        class: logging.StreamHandler
        formatter: simple
        level: DEBUG

  loggers:
      caes:
        level: DEBUG
        handlers: [console]
        propagate: false

  root:
    level: INFO
    handlers: [console]
```
