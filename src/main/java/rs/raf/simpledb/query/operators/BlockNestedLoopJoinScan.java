package rs.raf.simpledb.query.operators;

import rs.raf.simpledb.query.Constant;
import rs.raf.simpledb.tx.Transaction;
import rs.raf.simpledb.utils.BufferNeeds;
import rs.raf.simpledb.record.*;
import rs.raf.simpledb.operators.ChunkScan;

/*
  Scan klasa za Block Nested Loop Join operator.

  Algoritam:
  1. Ucitaj chunk (blok) iz unutrasnje relacije u memoriju
  2. Za svaku torku iz spoljasnje relacije:
     - Uporedi sa svim torkama u chunk-u
     - Ako se join uslov poklapa, vrati tu kombinaciju torki
  3. Kada se iscrpi chunk, ucitaj sledeci chunk i ponovi od koraka 2
  4. Kada se iscrpe svi chunk-ovi, zavrsi

 */
public class BlockNestedLoopJoinScan implements Scan {
    private Scan outerScan;         //Spoljasnja relacija
    private Scan innerChunkScan;    //Trenutni chunk unutrasnje relacije
    private TableInfo innerTi;      //Metapodaci unutrasnje relacije
    private String joinField1;      //Join polje iz spoljasnje relacije
    private String joinField2;      //Join polje iz unutrasnje relacije
    private Transaction tx;
    
    private int chunkSize;          //Velicina chunk-a u blokovima
    private int nextBlockNum;       //Sledeci blok za ucitavanje
    private int fileSize;           //Ukupan broj blokova u fajlu
    
    private boolean hasMoreOuter;   //Da li ima jos torki u spoljasnjoj relaciji
    
    /*
      Kreira scan za Block Nested Loop Join.
     */
    public BlockNestedLoopJoinScan(Scan outerScan, TableInfo innerTi, 
                                    String joinField1, String joinField2, Transaction tx) {
        this.outerScan = outerScan;
        this.innerTi = innerTi;
        this.joinField1 = joinField1;
        this.joinField2 = joinField2;
        this.tx = tx;
        
        //Odredi velicinu fajla i chunk-a
        this.fileSize = tx.size(innerTi.fileName());
        this.chunkSize = BufferNeeds.bestFactor(fileSize);
        if (this.chunkSize < 1) this.chunkSize = 1;
        
        beforeFirst();
    }
    
    /*
      Pozicionira scan pre prvog zapisa.
     */
    @Override
    public void beforeFirst() {
        nextBlockNum = 0;
        hasMoreOuter = true;
        outerScan.beforeFirst();
        hasMoreOuter = outerScan.next();
        useNextChunk();
    }
    
    /*
      Prelazi na sledeci zapis koji zadovoljava join uslov.
     */
    @Override
    public boolean next() {
        while (hasMoreOuter) {
            //Pokusaj da nadjes match u trenutnom chunk-u
            while (innerChunkScan != null && innerChunkScan.next()) {
                if (matchesJoinCondition()) {
                    return true;
                }
            }
            
            //Nema vise u chunk-u, predji na sledecu spoljasnju torku
            hasMoreOuter = outerScan.next();
            
            if (hasMoreOuter) {
                //Resetuj chunk za novu spoljasnju torku
                if (innerChunkScan != null) {
                    innerChunkScan.beforeFirst();
                }
            } else {
                //Nema vise spoljasnih torki, pokusaj sa sledecim chunk-om
                if (useNextChunk()) {
                    outerScan.beforeFirst();
                    hasMoreOuter = outerScan.next();
                } else {
                    return false; //Nema vise chunk-ova
                }
            }
        }
        return false;
    }
    
    /*
      Proverava da li se trenutne torke poklapaju po join uslovu.
     */
    private boolean matchesJoinCondition() {
        Constant outerVal = outerScan.getVal(joinField1);
        Constant innerVal = innerChunkScan.getVal(joinField2);
        return outerVal.equals(innerVal);
    }
    
    /*
      Ucitava sledeci chunk unutrasnje relacije.
     */
    private boolean useNextChunk() {
        if (innerChunkScan != null) {
            innerChunkScan.close();
        }
        
        if (nextBlockNum >= fileSize) {
            return false;
        }
        
        int endBlock = nextBlockNum + chunkSize - 1;
        if (endBlock >= fileSize) {
            endBlock = fileSize - 1;
        }
        
        innerChunkScan = new ChunkScan(innerTi, nextBlockNum, endBlock, tx);
        nextBlockNum = endBlock + 1;
        return true;
    }
    
    /*
      Zatvara sve otvorene scan-ove.
     */
    @Override
    public void close() {
        outerScan.close();
        if (innerChunkScan != null) {
            innerChunkScan.close();
        }
    }
    
    /*
      Vraca vrednost datog polja.
     */
    @Override
    public Constant getVal(String fldname) {
        if (outerScan.hasField(fldname)) {
            return outerScan.getVal(fldname);
        } else {
            return innerChunkScan.getVal(fldname);
        }
    }
    
    /*
      Vraca int vrednost datog polja.
     */
    @Override
    public int getInt(String fldname) {
        if (outerScan.hasField(fldname)) {
            return outerScan.getInt(fldname);
        } else {
            return innerChunkScan.getInt(fldname);
        }
    }
    
    /*
      Vraca string vrednost datog polja.
     */
    @Override
    public String getString(String fldname) {
        if (outerScan.hasField(fldname)) {
            return outerScan.getString(fldname);
        } else {
            return innerChunkScan.getString(fldname);
        }
    }
    
    /*
      Proverava da li scan sadrzi dato polje.
     */
    @Override
    public boolean hasField(String fldname) {
        return outerScan.hasField(fldname) || 
               (innerChunkScan != null && innerChunkScan.hasField(fldname));
    }
}
