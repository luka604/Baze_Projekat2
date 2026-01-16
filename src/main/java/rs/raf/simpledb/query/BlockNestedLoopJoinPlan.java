package rs.raf.simpledb.query;

import rs.raf.simpledb.SimpleDBEngine;
import rs.raf.simpledb.query.operators.BlockNestedLoopJoinScan;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.record.*;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.query.operators.UpdateScan;

/*
  Plan klasa za Block Nested Loop Join operator.

  Block Nested Loop Join radi tako sto:
  1.Ucita blok (chunk) torki iz unutrasnje relacije u memoriju
  2.Za svaku torku iz spoljasnje relacije, uporedi je sa svim torkama u bloku
  3.Ponovi za sledeci blok unutrasnje relacije

  Ovo je efikasnije od obicnog Nested Loop Join-a jer smanjuje broj citanja sa diska.

 */
public class BlockNestedLoopJoinPlan implements Plan {
    private Plan lhs;           //Spoljasnja relacija (outer)
    private Plan rhs;           //Unutrasnja relacija (inner)
    private String joinField1;  //Polje za join iz lhs
    private String joinField2;  //Polje za join iz rhs
    private Transaction tx;
    private Schema schema = new Schema();
    
    /*
      Kreira Block Nested Loop Join plan za date upite.
     */
    public BlockNestedLoopJoinPlan(Plan lhs, Plan rhs, String joinField1, String joinField2, Transaction tx) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinField1 = joinField1;
        this.joinField2 = joinField2;
        this.tx = tx;
        schema.addAll(lhs.schema());
        schema.addAll(rhs.schema());
    }
    
    /*
      Otvara scan za ovaj join.
      Materijalizuje unutrasnju relaciju u temp tabelu,
      zatim kreira BlockNestedLoopJoinScan.
     */
    @Override
    public Scan open() {
        // Materijalizuj unutrasnju relaciju (rhs) u temp tabelu
        TempTable tt = copyRecordsFrom(rhs);
        TableInfo ti = tt.getTableInfo();
        
        // Otvori spoljasnju relaciju
        Scan outerScan = lhs.open();
        
        // Kreiraj i vrati BlockNestedLoopJoinScan
        return new BlockNestedLoopJoinScan(outerScan, ti, joinField1, joinField2, tx);
    }
    
    /*
      Procenjuje broj pristupa blokovima.
      Formula: B(lhs) + (B(lhs) / M) * B(rhs)
      gde je M broj dostupnih bafera
     */
    @Override
    public int blocksAccessed() {
        int avail = SimpleDBEngine.bufferMgr().available();
        if (avail <= 1) avail = 2;
        
        Plan mp = new MaterializePlan(rhs, tx);
        int rhsBlocks = mp.blocksAccessed();
        int lhsBlocks = lhs.blocksAccessed();
        
        int numChunks = (int) Math.ceil((double) lhsBlocks / (avail - 1));
        return lhsBlocks + numChunks * rhsBlocks;
    }
    
    /*
      Procenjuje broj izlaznih torki.
      Za equi-join: R(lhs) * R(rhs) / max(V(lhs, joinField), V(rhs, joinField))
     */
    @Override
    public int recordsOutput() {
        int lhsRecords = lhs.recordsOutput();
        int rhsRecords = rhs.recordsOutput();
        int maxDistinct = Math.max(lhs.distinctValues(joinField1), rhs.distinctValues(joinField2));
        if (maxDistinct == 0) maxDistinct = 1;
        return (lhsRecords * rhsRecords) / maxDistinct;
    }
    
    /*
      Procenjuje broj razlicitih vrednosti za dato polje.
     */
    @Override
    public int distinctValues(String fldname) {
        if (lhs.schema().hasField(fldname))
            return lhs.distinctValues(fldname);
        else
            return rhs.distinctValues(fldname);
    }
    
    /*
      Vraca shemu rezultata (unija shema obe relacije).
     */
    @Override
    public Schema schema() {
        return schema;
    }
    
    /*
      Pomocna metoda za kopiranje torki u temp tabelu.
     */
    private TempTable copyRecordsFrom(Plan p) {
        Scan src = p.open();
        Schema sch = p.schema();
        TempTable tt = new TempTable(sch, tx);
        UpdateScan dest = (UpdateScan) tt.open();
        while (src.next()) {
            dest.insert();
            for (String fldname : sch.fields())
                dest.setVal(fldname, src.getVal(fldname));
        }
        src.close();
        dest.close();
        return tt;
    }
    
    @Override
    public void printPlan(int indentLevel) {
        System.out.println("-".repeat(indentLevel) + "-> BLOCK NESTED LOOP JOIN (" + joinField1 + "=" + joinField2 + ") OF ->");
        lhs.printPlan(indentLevel + 3);
        rhs.printPlan(indentLevel + 3);
    }
}
