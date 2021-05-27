package com.hazelcast.jet.tests.sql.common;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlRow;

import java.util.Iterator;
import java.util.concurrent.Callable;

import static java.util.concurrent.locks.LockSupport.parkNanos;

public class JoinByNonKeyExecutor extends BaseExecutor implements Callable<Integer> {

    private static final String MAP_NAME = "my_non_key_map";
    private static final int MAP_SIZE = 10000;

    private IMap<Key, Pojo> myMap;
    private ILogger logger;
    private long begin;
    private long durationInMillis;

    private JetInstance jetInstance;

    public JoinByNonKeyExecutor(JetInstance jetInstance, ILogger logger, long begin,
                                long durationInMillis, long threshold) {
        super(jetInstance, logger, begin, durationInMillis, threshold);
        this.jetInstance = jetInstance;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;

        myMap = jetInstance.getHazelcastInstance().getMap(MAP_NAME);
        createSqlMapping();
        populateMap(myMap, MAP_SIZE);
    }

    public Integer call() {
        logger.info("Execute query: " + getSqlQuery());
        Iterator<SqlRow> iterator = jetInstance.getSql().execute(getSqlQuery()).iterator();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            SqlRow sqlRow = iterator.next();
            long longValue = sqlRow.getObject("v");
            currentQueryCount++;
            verifyNotStuck();
            printProgress("join by non key");
            parkNanos(threshold);
        }
        return currentQueryCount;
    }

    public String getSqlQuery() {
        return "SELECT * FROM TABLE(generate_stream(10)) streaming LEFT JOIN " + MAP_NAME + " " +
                "AS map ON map.bigIntVal=streaming.v";
    }

    private void createSqlMapping() {
        jetInstance.getSql().execute("CREATE MAPPING " + MAP_NAME + "(" +
                " booleanVal BOOLEAN," +
                " tinyIntVal TINYINT," +
                " smallIntVal SMALLINT," +
                " intVal INT," +
                " bigIntVal BIGINT," +
                " realVal REAL," +
                " doubleVal DOUBLE," +
                " decimalVal DECIMAL," +
                " varcharVal VARCHAR)" +
                " TYPE IMap" +
                " OPTIONS (" +
                " 'keyFormat' = 'java'," +
                " 'keyJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Key'," +
                " 'valueFormat' = 'java'," +
                " 'valueJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Pojo'" +
                ")");
    }
}
