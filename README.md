# What is it?
A compaction library for HBase 0.96 -> Hbase 0.99. It implements [OpenTSDB Compaction algorithm](http://opentsdb.net/docs/build/html/user_guide/backends/hbase.html#compactions)

# Installation
Run Maven package
    mvn package
And copy the resulting jar file into HBase's CLASSPATH. This is usually defined in /etc/hbase/hbase-env.sh

Then change the configuration of the TSDB's 't' column family in hbase shell
    disable 'tsdb'
    alter 'tsdb', {NAME => 't', CONFIGURATION => {'hbase.hstore.defaultengine.compactor.class' => 'com.twilio.compaction.TSDCompactor'}}
    enable 'tsdb'
