/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.tdb3.txnr;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.dboe.transaction.txn.ComponentId;
import org.apache.jena.dboe.transaction.txn.SysTransState;
import org.apache.jena.dboe.transaction.txn.Transaction;
import org.apache.jena.dboe.transaction.txn.TransactionalComponent;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.seaborne.tdb3.rdata.RocksPrepare;
import org.seaborne.tdb3.sys.RocksTDB;

/**
 * {@link TransactionalComponent} for a RocksDB database.
 *
 * @implNote RocksDB itself provide the full transaction cycle for commit.
 * <p>
 * As of version Rocks 6.4.6, the API does not expose a prepare()-commit() pair,
 * only commit(). There is work-in-progress and when available, this component can
 * be adapted to use that. Then multiple databases, or multiple
 * different {@link TransactionalComponent TransactionalComponents}
 * can be combined into one DBOE transaction.
 */
public class TransactionalComponentR implements TransactionalComponent {

    private final org.rocksdb.OptimisticTransactionDB rocksTxnDB;
    private ThreadLocal<org.rocksdb.Transaction> thisTxn = ThreadLocal.withInitial(()->null);

    public TransactionalComponentR(RocksTDB rocksTDB) {
        this(rocksTDB.rdb);
    }

    private TransactionalComponentR(OptimisticTransactionDB rocksDB) {
        rocksTxnDB = rocksDB;
    }

    private List<RocksPrepare> items = new ArrayList<>();
    public void addPreparables(RocksPrepare item) {
        items.add(item);
    }

    static ComponentId componentId = ComponentId.create(null, StrUtils.asUTF8bytes("RDB"));

    @Override
    public ComponentId getComponentId() {
        return componentId;
    }

    @Override
    public void startRecovery() {}

    @Override
    public void recover(ByteBuffer ref) {}

    @Override
    public void finishRecovery() {}

    @Override
    public void cleanStart() {}

    @Override
    public void begin(Transaction transaction) {
        //FIXME
        WriteOptions wOptions = new WriteOptions();
        //When? wOptions.dispose();
        // Reuse org.rockdb.Transaction
        org.rocksdb.Transaction rTxn = rocksTxnDB.beginTransaction(wOptions);
        thisTxn.set(rTxn);
    }

    @Override
    public boolean promote(Transaction transaction) {
        return true;
    }

    @Override
    public ByteBuffer commitPrepare(Transaction transaction) {
        items.forEach(RocksPrepare::prepare);
        return null;
    }

    @Override
    public void commit(Transaction transaction) {
        org.rocksdb.Transaction rTxn = thisTxn.get();
        try {
            rTxn.commit();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void commitEnd(Transaction transaction) {
    }

    @Override
    public void abort(Transaction transaction) {
        org.rocksdb.Transaction rTxn = thisTxn.get();
        try {
            rTxn.rollback();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void complete(Transaction transaction) {
        @SuppressWarnings("resource")
        org.rocksdb.Transaction rTxn = thisTxn.get();
        rTxn.close();
        thisTxn.remove();
    }

    @Override
    public SysTransState detach() {
        return null;
    }

    @Override
    public void attach(SysTransState systemState) {}

    @Override
    public void shutdown() {
        try {
            rocksTxnDB.syncWal();
            rocksTxnDB.closeE();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        // FIXME Other .close();
    }

}
