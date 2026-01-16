package rs.raf.simpledb;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import rs.raf.simpledb.query.*;
import rs.raf.simpledb.query.aggregation.AggregationFn;
import rs.raf.simpledb.query.aggregation.CountFn;
import rs.raf.simpledb.query.aggregation.GroupByPlan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.tx.Transaction;

/*
  Student indeks: 98
  Zbir indeksa mod 3 = 2

  SQL UPIT 3:
  SELECT prednaziv, COUNT(ocena) AS desetke
  FROM PREDMET, SMER, ISPIT, POLAGANJE
  WHERE pid=predmetid
    AND ispid=ispitid
    AND smid=predsmerid
    AND smername='Softversko inzenjerstvo'
    AND ispugod = 2024
    AND ocena=10
  GROUP BY pid
  ORDER BY desetke

  OPERATOR: Block Nested Loop Join

 */
public class MainQueryRunner {

    public static void main(String[] args) {
        try {
            //Inicijalizacija baze
            boolean isnew = InitKolokvijumDB.initDB("studentdb");

            if (isnew) {
                System.out.println("Kreiranje nove baze podataka...");
                InitKolokvijumDB.createDBTables();
                InitKolokvijumDB.genericInsertDBData();
            } else {
                System.out.println("Baza podataka vec postoji.");
            }

            //DEO 1: Optimizovani plan izvrsavanja upita
            System.out.println("\nDEO 1: Plan izvrsavanja SQL upita 3");
            executeQuery3();

            //DEO 2: Demonstracija Block Nested Loop Join operatora
            System.out.println("\nDEO 2: Demonstracija Block Nested Loop Join operatora");
            demonstrateBlockNestedLoopJoin();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
      DEO 1: Implementacija optimizovanog plana za SQL Upit 3

      SQL UPIT 3:
      SELECT prednaziv, COUNT(ocena) AS desetke
      FROM PREDMET, SMER, ISPIT, POLAGANJE
      WHERE pid=predmetid
        AND ispid=ispitid
        AND smid=predsmerid
        AND smername='Softversko inzenjerstvo'
        AND ispugod = 2024
        AND ocena=10
      GROUP BY pid
      ORDER BY desetke

      OPTIMIZACIJE:
      1. Potiskivanje selekcije (push selection) - filteri se primenjuju sto pre
      2. Redosled join-ova - manje tabele prvo
      3. Koriscenje Block Nested Loop Join za efikasnije spajanje
     */
    private static void executeQuery3() {

        Transaction tx = new Transaction();

        //1.Kreiranje TablePlan za sve tabele
        Plan pSmer = new TablePlan("smer", tx);
        Plan pPredmet = new TablePlan("predmet", tx);
        Plan pIspit = new TablePlan("ispit", tx);
        Plan pPolaganje = new TablePlan("polaganje", tx);

        //2. POTISKIVANJE SELEKCIJE - filtriramo sto pre
        
        //Filter SMER: smername = 'Softversko inzenjerstvo'
        Expression lhsSmer = new FieldNameExpression("smername");
        Constant cSmer = new StringConstant("Softversko inzenjerstvo");
        Expression rhsSmer = new ConstantExpression(cSmer);
        Term tSmer = new Term(lhsSmer, rhsSmer);
        Predicate predSmer = new Predicate(tSmer);
        Plan pSmerFiltered = new SelectionPlan(pSmer, predSmer);

        //Filter ISPIT: ispugod = 2024
        Expression lhsGod = new FieldNameExpression("ispugod");
        Constant cGod = new IntConstant(2024);
        Expression rhsGod = new ConstantExpression(cGod);
        Term tGod = new Term(lhsGod, rhsGod);
        Predicate predGod = new Predicate(tGod);
        Plan pIspitFiltered = new SelectionPlan(pIspit, predGod);

        //Filter POLAGANJE: ocena = 10
        Expression lhsOcena = new FieldNameExpression("ocena");
        Constant cOcena = new IntConstant(10);
        Expression rhsOcena = new ConstantExpression(cOcena);
        Term tOcena = new Term(lhsOcena, rhsOcena);
        Predicate predOcena = new Predicate(tOcena);
        Plan pPolaganjeFiltered = new SelectionPlan(pPolaganje, predOcena);

        
        //3.Join SMER i PREDMET: smid = predsmerid
        Plan pJoin1 = new CrossProductPlan(pSmerFiltered, pPredmet);
        Expression lhsJ1 = new FieldNameExpression("smid");
        Expression rhsJ1 = new FieldNameExpression("predsmerid");
        Term tJ1 = new Term(lhsJ1, rhsJ1);
        Predicate predJ1 = new Predicate(tJ1);
        Plan pJoin1Filtered = new SelectionPlan(pJoin1, predJ1);

        //Join sa ISPIT: pid = predmetid
        Plan pJoin2 = new CrossProductPlan(pJoin1Filtered, pIspitFiltered);
        Expression lhsJ2 = new FieldNameExpression("pid");
        Expression rhsJ2 = new FieldNameExpression("predmetid");
        Term tJ2 = new Term(lhsJ2, rhsJ2);
        Predicate predJ2 = new Predicate(tJ2);
        Plan pJoin2Filtered = new SelectionPlan(pJoin2, predJ2);

        //Join sa POLAGANJE: ispid = ispitid
        Plan pJoin3 = new CrossProductPlan(pJoin2Filtered, pPolaganjeFiltered);
        Expression lhsJ3 = new FieldNameExpression("ispid");
        Expression rhsJ3 = new FieldNameExpression("ispitid");
        Term tJ3 = new Term(lhsJ3, rhsJ3);
        Predicate predJ3 = new Predicate(tJ3);
        Plan pJoin3Filtered = new SelectionPlan(pJoin3, predJ3);

        //4.GROUP BY pid sa COUNT(ocena)
        Collection<String> groupFields = Arrays.asList("pid","prednaziv");
        AggregationFn countAggr = new CountFn("ocena");
        Collection<AggregationFn> aggFunctions = Arrays.asList(countAggr);
        Plan pGroupBy = new GroupByPlan(pJoin3Filtered, groupFields, aggFunctions, tx);

        //5.ORDER BY desetke (countofocena)
        List<String> sortFields = Arrays.asList("countofocena");
        Plan pFinal = new SortPlan(pGroupBy, sortFields, tx);

        //Prikaz logickog plana
        System.out.println("\nOPTIMIZOVANI LOGICKI PLAN:");
        pFinal.printPlan(0);

        //Izvrsavanje upita
        System.out.println("REZULTATI UPITA:");
        System.out.printf("%-30s %s%n", "Predmet", "Broj desetki");


        Scan s = pFinal.open();
        int totalRows = 0;
        while (s.next()) {
            String predNaziv = s.getString("prednaziv");
            int desetke = s.getInt("countofocena");
            System.out.printf("%-30s %d%n", predNaziv, desetke);
            totalRows++;
        }
        s.close();
        tx.commit();

        System.out.println("-".repeat(45));
        System.out.println("Ukupno predmeta: " + totalRows);
    }

    /*
      DEO 2:Demonstracija rada Block Nested Loop Join operatora
      Prikazuje kako Block Nested Loop Join radi na jednostavnom primeru
     */
    private static void demonstrateBlockNestedLoopJoin() {

        System.out.println("\nPrimer: Join SMER i PREDMET po polju smid=predsmerid");
        System.out.println("Filtriramo samo Softversko inzenjerstvo i predmete prve godine\n");

        Transaction tx = new Transaction();

        //Kreiranje tabela
        Plan pSmer = new TablePlan("smer", tx);
        Plan pPredmet = new TablePlan("predmet", tx);

        //Filter SMER: smername = 'Softversko inzenjerstvo'
        Expression lhsSmer = new FieldNameExpression("smername");
        Constant cSmer = new StringConstant("Softversko inzenjerstvo");
        Expression rhsSmer = new ConstantExpression(cSmer);
        Term tSmer = new Term(lhsSmer, rhsSmer);
        Predicate predSmer = new Predicate(tSmer);
        Plan pSmerFiltered = new SelectionPlan(pSmer, predSmer);

        //Filter PREDMET: predgod = 1 (prva godina)
        Expression lhsGod = new FieldNameExpression("predgod");
        Constant cGod = new IntConstant(1);
        Expression rhsGod = new ConstantExpression(cGod);
        Term tGod = new Term(lhsGod, rhsGod);
        Predicate predGod = new Predicate(tGod);
        Plan pPredmetFiltered = new SelectionPlan(pPredmet, predGod);

        //Koristimo Block Nested Loop Join
        Plan pBlockJoin = new BlockNestedLoopJoinPlan(
            pSmerFiltered, 
            pPredmetFiltered, 
            "smid",         //join field iz SMER
            "predsmerid",   //join field iz PREDMET
            tx
        );

        //Projekcija
        List<String> fields = Arrays.asList("smername", "prednaziv", "predgod");
        Plan pFinal = new ProjectionPlan(pBlockJoin, fields);

        //Prikaz plana
        System.out.println("LOGICKI PLAN SA BLOCK NESTED LOOP JOIN:");
        pFinal.printPlan(0);

        //Izvrsavanje
        System.out.println("\nREZULTATI:");
        System.out.printf("%-25s %-25s %s%n", "Smer", "Predmet", "Godina");
        System.out.println("-".repeat(60));

        Scan s = pFinal.open();
        int count = 0;
        while (s.next() && count < 15) {
            String smerName = s.getString("smername");
            String predNaziv = s.getString("prednaziv");
            int predGod2 = s.getInt("predgod");
            System.out.printf("%-25s %-25s %d%n", smerName, predNaziv, predGod2);
            count++;
        }
        if (count == 15) {
            System.out.println("... (prikazano prvih 15 rezultata)");
        }
        s.close();
        tx.commit();
    }

    /*
     Pomocna metoda za izvrsavanje SQL UPDATE naredbi
     */
    public static int executeSQLUpdate(String sqlCmd) {
        Transaction tx = null;
        try {
            tx = new Transaction();
            int result = SimpleDBEngine.planner().executeUpdate(sqlCmd, tx);
            tx.commit();
            return result;
        } catch (RuntimeException e) {
            if (tx != null) tx.rollback();
            throw e;
        }
    }
}
