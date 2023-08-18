package org.seaborne.tdb3;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Timer;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetOps;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
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
        //String DATA = "/home/afs/Datasets/BSBM/bsbm-5m.nt.gz";
        String DATA = "/home/rushikesh/workspace/genesis-prototype/data-generator/output/experiments-1.nt.gz";

        int N = 100_000;
        BuildR.batchSizeIndex = N;
        BuildR.batchSizeNodeTable = N;

        if ( cleanStart ) {
            FileOps.clearAll(tdb2DIR);
            FileOps.clearAll(tdb3DIR);
        }

        DatasetGraph tdb3dsg = DatabaseBuilderTDB3.build(Location.create(tdb3DIR), StoreParams.getDftStoreParams());
        DatasetGraph tdb2dsg = TDB2StorageBuilder.build(Location.create(tdb2DIR));

        double seconds = 0;
        DatasetGraph[] datasetGraphs = {tdb2dsg, tdb3dsg};
        for(DatasetGraph datasetGraph: datasetGraphs){
            System.out.printf(datasetGraph.getClass().getName() + " Write Start .... Batch size = %,d\n",N);
            long z = Timer.time(()->{
                Txn.executeWrite(datasetGraph,  ()->{
                    StreamRDF dest = StreamRDFLib.dataset(datasetGraph);
                    AsyncParser.asyncParse(DATA, dest);
                });
            });
            seconds = (z/1000.0);
            System.out.printf(datasetGraph.getClass().getName() + " Total load time  = %,.3f s\n", seconds);
        }

        long z1 = Timer.time(()->{
            ((DatasetGraphTDB3)tdb3dsg).compact();
        });
        double compactionSeconds = (z1/1000.0);
        System.out.printf("Compaction = %,.3f s\n", compactionSeconds);

        for(DatasetGraph datasetGraph: datasetGraphs) {
            long z = Timer.time(()->{
                int x =
                        Txn.calculateRead(datasetGraph, () -> {
                            return datasetGraph.getDefaultGraph().size();
                        });
                System.out.printf(datasetGraph.getClass().getName() + " Count = %,d\n", x);
            });
            seconds = (z/1000.0);
            System.out.printf(datasetGraph.getClass().getName() + " Total Read time  = %,.3f s\n", seconds);
        }


//        RowSet rowSet1 = QueryExec.dataset(dsg).query("SELECT (count(*) AS ?C) { ?s ?p ?o }").select();
//        RowSetOps.out(rowSet1);

//        RowSet rowSet2 = QueryExec.dataset(dsg).query("SELECT DISTINCT ?s { ?s ?p ?o } LIMIT 10").select();
//        RowSetOps.out(rowSet2);

//        RowSet rowSet3 = QueryExec.dataset(dsg).query("SELECT * { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1> ?p ?o }").select();
//        RowSetOps.out(rowSet3);
    }
}
