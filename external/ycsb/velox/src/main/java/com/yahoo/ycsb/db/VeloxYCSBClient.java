package com.yahoo.ycsb.db;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import edu.berkeley.velox.datamodel.Key;
import edu.berkeley.velox.datamodel.Value;
import edu.berkeley.velox.frontend.VeloxConnection;
import org.apache.log4j.Logger;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;

public class VeloxYCSBClient extends DB {
    private final Logger logger = Logger.getLogger(VeloxYCSBClient.class);

    private static Boolean initializationFinished = false;
    private static Boolean initializationStarted = false;

    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;

    public static final AtomicInteger inserts = new AtomicInteger();

    // shared between all YCSB clients on this JVM
    private static VeloxConnection connection;

	public void init() throws DBException {
        synchronized (initializationFinished) {
            if(!initializationFinished) {
                if(!initializationStarted) {
                    initializationStarted = true;
                } else {
                    try {
                        initializationFinished.wait();
                    } catch (InterruptedException e) {
                    }
                    return;
                }
            } else {
                return;
            }
        }

        String hosts = getProperties().getProperty("cluster");

        if(hosts == "null") throw new DBException("Missing 'cluster' property!");

        String[] hostsArr = hosts.split(",");
        List<InetSocketAddress> hostAddresses = new ArrayList<InetSocketAddress>();
        for(String hostStr : hostsArr) {
            hostAddresses.add(new InetSocketAddress(hostStr.split(":")[0], Integer.parseInt(hostStr.split(":")[1])));
        }

        connection = VeloxConnection.makeConnection(hostAddresses);

        synchronized (initializationFinished) {
            initializationFinished.notifyAll();
        }
	}
	
	public void cleanup() throws DBException {
	}
	
	@Override
	public int delete(String table, String key) {
        connection.putValue(new Key(key.hashCode()), new Value(""));
        return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        String toWrite = null;

        // in insert
        if(!values.values().iterator().hasNext()) {
            toWrite = "";
        } else {
            toWrite = values.values().iterator().next().toString();
        }

        connection.putValue(new Key(key.hashCode()), new Value(toWrite));

		return OK;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
        connection.getValue(new Key(key.hashCode()));

		return OK;
	}

	@Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
                    Vector<HashMap<String,ByteIterator>> result) {
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		return insert(table, key, values);
	}
}
