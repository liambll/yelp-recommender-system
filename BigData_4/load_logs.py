# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
import os, sys, gzip, re
import datetime

def main(argv=None):
    if argv is None:
        inputs = sys.argv[1]
        keyspace = sys.argv[2]
        table = sys.argv[3]
        
    cluster = Cluster(['199.60.17.136', '199.60.17.173'])
    session = cluster.connect(keyspace)

    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    for f in os.listdir(inputs):
        with gzip.GzipFile(os.path.join(inputs, f)) as logfile:
            count = 0
            statement = "BEGIN BATCH "
            for line in logfile:
                lineSplit = linere.split(line.replace("'","_"))
                if len(lineSplit) > 4:
                    statement = statement + \
                        "INSERT INTO " + table + " (id, host, datetime, path, bytes) " +\
                        "VALUES (UUID(), '" + lineSplit[1] + "', '" + \
                        datetime.datetime.strptime(lineSplit[2], '%d/%b/%Y:%H:%M:%S').isoformat() + "', '" + \
                        lineSplit[3] + "', " + lineSplit[4] + ");"
                    count += 1
                    if count >= 300:
                        statement = statement + "APPLY BATCH;"
                        #print("------" + statement)
                        session.execute(statement)
                        count = 0
                        statement = "BEGIN BATCH "

            statement = statement + "APPLY BATCH;"
            #print("------" + statement)
            session.execute(statement)            
    cluster.shutdown()
        
if __name__ == "__main__":
    main()