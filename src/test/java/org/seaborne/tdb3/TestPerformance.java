package org.seaborne.tdb3;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Timer;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.params.StoreParams;

import org.apache.jena.tdb2.store.TDB2StorageBuilder;
import org.junit.Test;
import org.seaborne.tdb3.sys.BuildR;

public class TestPerformance {
    @Test
    public void testTDB2vsTDB3() {
        String tdb2DIR = "/home/rushikesh/workspace/db/tdb2";
        String tdb3DIR = "/home/rushikesh/workspace/db/tdb3";
        FileOps.ensureDir(tdb2DIR);
        FileOps.ensureDir(tdb3DIR);
        boolean cleanStart = true;

        String DATA = "/home/rushikesh/workspace/genesis-prototype/output-100/experiments-1.nt";

        int batchWriteSize = 100_000;
        BuildR.batchSizeIndex = batchWriteSize;
        BuildR.batchSizeNodeTable = batchWriteSize;

        if (cleanStart) {
            FileOps.clearAll(tdb2DIR);
            FileOps.clearAll(tdb3DIR);
        }

        DatasetGraph tdb3dsg = DatabaseBuilderTDB3.build(Location.create(tdb3DIR), StoreParams.getDftStoreParams());
        DatasetGraph tdb2dsg = TDB2StorageBuilder.build(Location.create(tdb2DIR));
        DatasetGraph[] datasetGraphs = {tdb2dsg, tdb3dsg};

        measureWritePerformance(DATA, datasetGraphs, batchWriteSize);
        compactTDB3(tdb3dsg);
        measureReadPerformance(datasetGraphs);

    }

    private static void measureWritePerformance(String data, DatasetGraph[] datasetGraphs, int batchSize) {
        double seconds = 0;
        for (DatasetGraph datasetGraph : datasetGraphs) {
            System.out.printf(datasetGraph.getClass().getName() + " Write Start .... Batch size = %,d\n", batchSize);
            long z = Timer.time(() -> {
                Txn.executeWrite(datasetGraph, () -> {
                    StreamRDF dest = StreamRDFLib.dataset(datasetGraph);
                    AsyncParser.asyncParse(data, dest);
                });
            });
            seconds = (z / 1000.0);
            System.out.printf(datasetGraph.getClass().getName() + " Total load time  = %,.3f s\n", seconds);
        }
    }

    private static void compactTDB3(DatasetGraph tdb3dsg) {
        long z1 = Timer.time(() -> {
            ((DatasetGraphTDB3) tdb3dsg).compact();
        });
        double compactionSeconds = (z1 / 1000.0);
        System.out.printf("Compaction = %,.3f s\n", compactionSeconds);
    }


    private static void measureReadPerformance(DatasetGraph[] datasetGraphs) {
        double seconds = 0;
        for (DatasetGraph datasetGraph : datasetGraphs) {
            long z = Timer.time(() -> {
                int x =
                        Txn.calculateRead(datasetGraph, () -> {
                            return datasetGraph.getDefaultGraph().size();
                        });
                System.out.printf(datasetGraph.getClass().getName() + " Count = %,d\n", x);
            });
            seconds = (z / 1000.0);
            System.out.printf(datasetGraph.getClass().getName() + " Total Read time  = %,.3f s\n", seconds);
        }
    }
}
