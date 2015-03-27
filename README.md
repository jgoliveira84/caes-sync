# Caes-Sync
Simple daemon to sync data between Elastic Search and Cassandra.

## Installing

Download this repository and do:
```shell 
$ cd <caes-sync-location>
$ python setup.py install
```

This will install caes-sync and all its dependencies.

## Running

We assume that you have already setup both an [ElasticSearch](https://www.elastic.co/ "Elastic Search") instance and a [Cassandra](http://cassandra.apache.org/ "Cassandra") one. Once caes-sync is instaled, you need an Yaml config file at ~/.caes/config.yaml where you are goint to put ElasticSearch/Cassandra conection parameters and other caes-sync stuff. This file's syntax is described in the *Configuring* section.

To make data syncing between between heterogeneous technologies like ElasticSearch and Cassandra possible, you need to conform your data to certain guidelines

##### Elastic Search 'schema'

##### Cassandra 'schema'





