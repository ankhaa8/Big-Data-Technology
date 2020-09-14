mkdir /tmp/input
mkdir /tmp/spooldir/
mkdir /tmp/spooldir/finished/
mkdir /tmp/error


  flume-ng agent --conf /home/cloudera/cs523/flume/conf --conf-file /home/cloudera/cs523/flume/conf/myFlumeConf.conf --name a1 -Dflume.root.logger=INFO,console
