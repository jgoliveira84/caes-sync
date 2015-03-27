# Caes-Sync
Simple daemon to sync data between ElasticSearch and Cassandra.

## Installing

Download this repository, extract it, go to its root and do:
```shell 
$ python setup.py install
```

This will install caes-sync and all its dependencies.

## Running

We assume that you have already setup both an [ElasticSearch](https://www.elastic.co/ "Elastic Search") instance and a [Cassandra](http://cassandra.apache.org/ "Cassandra") one. Once caes-sync is instaled, you need an Yaml config file at ~/.caes/config.yaml where you are going to put ElasticSearch/Cassandra conection parameters and other caes-sync stuff. This file's semantics is described in the *Configuring* section.

To start caes-sync do:

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

We also need to garantue consistency across both 'databases'. For this we will rely on an optimistyc consistency control approach and use ElasticSearch's 'built-in' field *_version* along with *_version_type=external*. We are going to provide **the same value to _timestamp and _version**. So, your inserts should look something like that:

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
  <timeseries_id_fieldname> int,
  timestamp int,
  <data_id_fieldname> uuid,
  PRIMARY KEY(<timeseries_id_fieldname>, timestamp)
);
```




