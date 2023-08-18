/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package dev;

import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.Timer;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.exec.RowSetOps;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb2.params.StoreParams;
import org.seaborne.tdb3.DatabaseBuilderTDB3;
import org.seaborne.tdb3.DatasetGraphTDB3;
import org.seaborne.tdb3.sys.BuildR;
import org.apache.jena.tdb2.TDB2.*;


public class DevTDB3 {
    // Todo:
    //  Try with no indexes(!) - node table.
    //  Node table - not via a RangeIndex.

    // DBOE
    //   Split StoreParams.
    //   Prefixes.
    //   TupleDB framework
    //   UnionGraph - no prefixes.
    //   Simplify ComponentId
    // TranactionCoordinator without(!) journal.

    // TDB3
    //   Tests
    //   Tidy.
    //   Merge back - update TDB2.
    //   Low-level filters.
    //   Loader
    //   Assembler
    //   Define the interface database as tuples.
    //   RocksRangeIndex iterator. BatchingIterator.
    // Issue copying bytes?

    // RocksDB
    //   Block cache
    //   Delay/stop compaction during a large load.

    // J4 - sort out prefixes for DatasetGraphs.

    static {
        LogCtl.setLogging();
        //LogCtl.setJavaLogging();
    }

    static int N = 100_000;
    // BSBM 5m:
    // 100000 : 110-115 seconds
    // 10000  : 105s
    // 5000   : 93.3
    // 1000   : 91s ****
    // 100    : 98
    // 10     : 102
    // 0      : 167.256

    public static void main(String...args) {
        BuildR.batchSizeIndex = N;
        BuildR.batchSizeNodeTable = N;
        // On / bsbm-1m:
        // 0 - off - 28k
        // 1 =>
        // 10 =>
        // 100 => 54k,59k
        // 1000 =>
        // 10000 =>

        // 5m : 6K (batch 100k)

        // Split numbers?
        // "Mode"

        // TDB2:
        // bsbm-1m: 47k
        main1();
    }

    public static void main1(String...args) {
        String DIR = "DB3";
        FileOps.ensureDir(DIR);
        boolean cleanStart = true;
        //String DATA = "/home/afs/Datasets/BSBM/bsbm-5m.nt.gz";
        String DATA = "/home/afs/Datasets/BSBM/bsbm-25m.nt.gz";

        if ( cleanStart )
            FileOps.clearAll(DIR);

//        System.out.println("Ready...");
//        try {
//            System.in.read();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        DatasetGraph dsg = DatabaseBuilderTDB3.build(Location.create(DIR), StoreParams.getDftStoreParams());
        //DatasetGraph dsg = DatabaseMgr.connectDatasetGraph(DIR);

        System.out.printf("Start .... Batch size = %,d\n",N);

//        long z = Timer.time(()->{
//            Txn.executeWrite(dsg,  ()->{
//                // Async parser.
//                // Batch load.
//                // Parallelize writes
//                // Avoiding Memtable
//                // Turn off compactions.
//                RDFDataMgr.read(dsg, DATA);
//            });
//        });

        long z = Timer.time(()->{
            Txn.executeWrite(dsg,  ()->{
                StreamRDF dest = StreamRDFLib.dataset(dsg);
                AsyncParser.asyncParse(DATA, dest);
            });
        });

        double seconds = (z/1000.0);
        System.out.printf("Load time  = %,.3f s\n", seconds);
        DatasetGraphTDB3 dsg3 =  (DatasetGraphTDB3)dsg;
        long z1 = Timer.time(()->{
            dsg3.compact();
        });
        double compactionSeconds = (z1/1000.0);
        System.out.printf("Compaction = %,.3f s\n", compactionSeconds);
        int x =
            Txn.calculateRead(dsg,  ()->{
                return dsg.getDefaultGraph().size();
            });
        System.out.printf("Count = %,d\n", x);
        System.out.printf("Rate  = %,.3f TPS\n", x/seconds);


        RowSet rowSet1 = QueryExec.dataset(dsg).query("SELECT (count(*) AS ?C) { ?s ?p ?o }").select();
        RowSetOps.out(rowSet1);

//        RowSet rowSet2 = QueryExec.dataset(dsg).query("SELECT DISTINCT ?s { ?s ?p ?o } LIMIT 10").select();
//        RowSetOps.out(rowSet2);

        RowSet rowSet3 = QueryExec.dataset(dsg).query("SELECT * { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1> ?p ?o }").select();
        RowSetOps.out(rowSet3);
    }

    public static void main0(String...args) {
        String DIR = "DB3";
        FileOps.ensureDir(DIR);
        boolean cleanStart = true;

        if ( cleanStart )
            FileOps.clearAll(DIR);

        DatasetGraph dsg = DatabaseBuilderTDB3.build(Location.create(DIR), StoreParams.getDftStoreParams());

        Node g1 = SSE.parseNode(":graph1");
        Node g2 = SSE.parseNode(":graph2");
        Node s = SSE.parseNode(":s");
        Node p = SSE.parseNode(":p");
        Node n1 = SSE.parseNode("1");
        Node n2 = SSE.parseNode("2");
        Node n3 = NodeFactory.createLiteral(DateTimeUtils.nowAsXSDDateTimeString());

        if ( cleanStart ) {
            Txn.executeWrite(dsg,  ()->{
                dsg.getDefaultGraph().getPrefixMapping().setNsPrefix("ex", "http://example/");
                dsg.add(Quad.defaultGraphIRI, s, p, n1);
                dsg.add(g1, s, p, n1);
                dsg.add(g2, s, p, n1);
                dsg.add(g1, s, p, n2);
                dsg.add(g2, s, p, n2);

                //Force batch!
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);
                dsg.add(g2, s, p, n2);

            });
        }

        Txn.executeWrite(dsg,  ()->{
            dsg.add(g2, s, p, n3);
        });

        Txn.executeRead(dsg,  ()->{
            RDFDataMgr.write(System.out, dsg, Lang.TRIG);
//            Iterator<Quad> iter = dsg.find(null, null, null, null);
//            iter.forEachRemaining(System.out::println);
            System.out.println("- - - - - - - - - - - -");
            RDFDataMgr.write(System.out, dsg.getUnionGraph(), Lang.TRIG);
//            dsg.getUnionGraph().find().forEachRemaining(System.out::println);
        });
    }
}
