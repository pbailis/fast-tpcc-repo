/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.berkeley.velox.util.zk;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Slightly adapted class from Apache Curator's DistributedDoubleBarrier recipe.
 * This is a single barrier that blocks until enough processes have joined the barrier.
 * However, there is no blocking to leave the barrier. The barrier must be reset
 * before each use.
 *
 */
public class DistributedCountingSingleBarrier
{
    private final CuratorFramework client;
    private final String barrierPath;
    private final int memberQty;
    private final String ourPath;
    private final String readyPath;
    private final AtomicBoolean hasBeenNotified = new AtomicBoolean(false);
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            connectionLost.set(event.getState() != Event.KeeperState.SyncConnected);
            notifyFromWatcher();
        }
    };

    private static final String     READY_NODE = "ready";

    /**
     * Creates the barrier abstraction. <code>memberQty</code> is the number of members in the
     * barrier. When {@link #enter()} is called, it blocks until all members have entered. When
     * {@link #leave()} is called, it blocks until all members have left.
     *
     * @param client the client
     * @param barrierPath path to use
     * @param memberQty the number of members in the barrier. NOTE: more than <code>memberQty</code>
     *                  can enter the barrier. <code>memberQty</code> is a threshold, not a limit
     */
    public DistributedCountingSingleBarrier(CuratorFramework client, String barrierPath, int memberQty)
    {
        Preconditions.checkState(memberQty > 0, "memberQty cannot be 0");

        this.client = client;
        this.barrierPath = barrierPath;
        this.memberQty = memberQty;
        ourPath = ZKPaths.makePath(barrierPath, UUID.randomUUID().toString());
        readyPath = ZKPaths.makePath(barrierPath, READY_NODE);
    }

    // the user can supply data to be stored at the barrier
    public synchronized void         resetBarrier() throws Exception
    {
        // reset barrier first in case the client has not removed it
        removeBarrier();
        try
        {
            client.create().creatingParentsIfNeeded().forPath(barrierPath);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore
        }
    }

    /**
     * Utility to remove the barrier node
     *
     * @throws Exception errors
     */
    public synchronized void      removeBarrier() throws Exception
    {
        try
        {
            client.delete().deletingChildrenIfNeeded().forPath(barrierPath);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }
    }

    /**
     * Enter the barrier and block until all members have entered
     *
     * @throws Exception interruptions, errors, etc.
     */
    public void     waitOnBarrier() throws Exception
    {
        waitOnBarrier(-1, null);
    }

    /**
     * Enter the barrier and block until all members have entered or the timeout has
     * elapsed
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if the entry was successful, false if the timeout elapsed first
     * @throws Exception interruptions, errors, etc.
     */
    public boolean     waitOnBarrier(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        // check if barrier already has enough members, and thus has signalled that it is ready
        boolean         readyPathExists = (client.checkExists().usingWatcher(watcher).forPath(readyPath) != null);
        // create ourPath znode, signalling that we have entered the barrier
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ourPath);

        boolean         result = (readyPathExists || internalWaitOnBarrier(startMs, hasMaxWait, maxWaitMs));
        if ( connectionLost.get() )
        {
            throw new KeeperException.ConnectionLossException();
        }

        return result;
    }

    @VisibleForTesting
    protected List<String> getChildrenForEntering() throws Exception
    {
        return client.getChildren().forPath(barrierPath);
    }

//    private List<String> filterAndSortChildren(List<String> children)
//    {
//        Iterable<String> filtered = Iterables.filter
//                (
//                        children,
//                        new Predicate<String>()
//                        {
//                            @Override
//                            public boolean apply(String name)
//                            {
//                                return !name.equals(READY_NODE);
//                            }
//                        }
//                );
//
//        ArrayList<String> filteredList = Lists.newArrayList(filtered);
//        Collections.sort(filteredList);
//        return filteredList;
//    }
//
//    private void checkDeleteOurPath(boolean shouldExist) throws Exception
//    {
//        if ( shouldExist )
//        {
//            client.delete().forPath(ourPath);
//        }
//    }

    private synchronized boolean internalWaitOnBarrier(long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        boolean result = true;
        do
        {
            List<String>    children = getChildrenForEntering();
            int             count = (children != null) ? children.size() : 0;
            if ( count >= memberQty )
            {
                try
                {
                    client.create().forPath(readyPath);
                }
                catch ( KeeperException.NodeExistsException ignore )
                {
                    // ignore
                }
                break;
            }

            if ( hasMaxWait && !hasBeenNotified.get() )
            {
                long        elapsed = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsed;
                if ( thisWaitMs <= 0 )
                {
                    result = false;
                }
                else
                {
                    wait(thisWaitMs);
                }

                if ( !hasBeenNotified.get() )
                {
                    result = false;
                }
            }
            else
            {
                wait();
            }
        } while ( false );

        return result;
    }

    private synchronized void notifyFromWatcher()
    {
        hasBeenNotified.set(true);
        notifyAll();
    }
}
