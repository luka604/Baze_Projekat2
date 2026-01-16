# Kolokvijum II - Napredne Baze Podataka
## Student indeks: 98 (mod 3 = 2)

---

## DEO 1: Plan izvršavanja SQL upita 3

### SQL Upit 3:
```sql
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
```

---

### 1. NEOPTIMIZOVANI PLAN IZVRŠAVANJA

Osnovni (naivni) plan bez optimizacija bi bio:

```
ORDER BY (desetke)
  |
GROUP BY (pid)
  |
SELECTION (smername='Softversko inzenjerstvo' AND ispugod=2024 AND ocena=10 
           AND pid=predmetid AND ispid=ispitid AND smid=predsmerid)
  |
CROSS PRODUCT
  |-- CROSS PRODUCT
  |     |-- CROSS PRODUCT
  |     |     |-- PREDMET
  |     |     |-- SMER
  |     |-- ISPIT
  |-- POLAGANJE
```

**Problem:** Dekartov proizvod svih tabela pre filtriranja generiše ogroman broj torki.
- PREDMET: ~167 torki
- SMER: 3 torke
- ISPIT: ~2004 torke
- POLAGANJE: ~5656 torki

Bez optimizacije: 167 × 3 × 2004 × 5656 ≈ 5.67 milijardi kombinacija!

---

### 2. OPTIMIZOVANI PLAN IZVRŠAVANJA

#### Primenjene transformacije relacione algebre:

**Transformacija 1: Potiskivanje selekcije (Push Selection Down)**
- σ(smername='Softversko inzenjerstvo') → primenjuje se direktno na SMER
- σ(ispugod=2024) → primenjuje se direktno na ISPIT
- σ(ocena=10) → primenjuje se direktno na POLAGANJE

**Transformacija 2: Optimalan redosled join-ova**
- Prvo spajamo manje tabele (SMER ima samo 3 torke, nakon filtera 1)
- Redosled: (SMER ⋈ PREDMET) ⋈ ISPIT ⋈ POLAGANJE

**Optimizovani plan:**

```
ORDER BY (countofocena)
  |
GROUP BY (prednaziv) + COUNT(ocena)
  |
SELECTION (ispid=ispitid)
  |
CROSS PRODUCT
  |-- SELECTION (pid=predmetid)
  |     |
  |     CROSS PRODUCT
  |       |-- SELECTION (smid=predsmerid)
  |       |     |
  |       |     CROSS PRODUCT
  |       |       |-- SELECTION (smername='Softversko inzenjerstvo')
  |       |       |     |-- TABLE SCAN (SMER)
  |       |       |-- TABLE SCAN (PREDMET)
  |       |-- SELECTION (ispugod=2024)
  |             |-- TABLE SCAN (ISPIT)
  |-- SELECTION (ocena=10)
        |-- TABLE SCAN (POLAGANJE)
```

---

### 3. PROCENA KARDINALNOSTI (broj torki na izlazu operatora)

#### Početne kardinalnosti tabela:
| Tabela | R (broj torki) | 
|--------|----------------|
| SMER | 3 |
| PREDMET | 167 |
| ISPIT | 2004 |
| POLAGANJE | 5656 |

#### Selektivnost i kardinalnost po operatorima:

**1. σ(smername='Softversko inzenjerstvo')(SMER)**
- Selektivnost: 1/V(smername) = 1/3
- Kardinalnost: 3 × (1/3) = **1 torka**

**2. σ(ispugod=2024)(ISPIT)**
- V(ispugod) ≈ 2 (godine 2024, 2025)
- Selektivnost: 1/2
- Kardinalnost: 2004 × (1/2) = **~1002 torke**

**3. σ(ocena=10)(POLAGANJE)**
- V(ocena) = 5 (ocene 6,7,8,9,10)
- Selektivnost: 1/5
- Kardinalnost: 5656 × (1/5) = **~1131 torki**

**4. SMER_filtered ⋈ PREDMET (smid=predsmerid)**
- Formula: R(S) × R(P) / max(V(S,smid), V(P,predsmerid))
- = 1 × 167 / max(1, 3) = 167/3 = **~56 torki**
- (predmeti sa smera Softversko inženjerstvo)

**5. (SMER⋈PREDMET) ⋈ ISPIT_filtered (pid=predmetid)**
- = 56 × 1002 / max(V(pid), V(predmetid))
- = 56 × 1002 / 167 = **~336 torki**

**6. (...) ⋈ POLAGANJE_filtered (ispid=ispitid)**
- = 336 × 1131 / max(V(ispid), V(ispitid))
- = 336 × 1131 / 2004 = **~190 torki**

**7. GROUP BY (pid)**
- Kardinalnost = broj različitih vrednosti pid u rezultatu
- ≈ **10-20 grupa** (predmeti sa desetkama)

**8. ORDER BY**
- Ne menja kardinalnost, samo sortira
- Kardinalnost: **~10-20 torki**

---

### 4. FORMULE ZA SELEKTIVNOST

| Operator | Formula |
|----------|---------|
| σ(A=c) | 1/V(A) |
| σ(A=B) (join) | 1/max(V(R,A), V(S,B)) |
| R × S | R(R) × R(S) |
| R ⋈ S | R(R) × R(S) / max(V(R,A), V(S,A)) |
| GROUP BY | V(grouping_columns) |

---

## DEO 2: Block Nested Loop Join Operator

### Opis algoritma:

Block Nested Loop Join je optimizovana verzija Nested Loop Join algoritma koja smanjuje broj I/O operacija tako što:

1. Učitava **blok (chunk)** torki iz unutrašnje relacije u memoriju
2. Za svaku torku iz spoljašnje relacije, upoređuje je sa **svim torkama u bloku**
3. Kada se iscrpi blok, učitava sledeći blok i ponavlja

### Pseudokod:
```
for each block B of inner relation:
    load B into memory
    for each tuple r in outer relation:
        for each tuple s in B:
            if r.joinField == s.joinField:
                output (r, s)
```

### Složenost:
- **I/O cost:** B(outer) + ⌈B(outer)/(M-1)⌉ × B(inner)
- Gde je M broj dostupnih bafera

### Implementacija:

Implementirane su dve klase:
1. **BlockNestedLoopJoinPlan.java** - Plan klasa (logički operator)
2. **BlockNestedLoopJoinScan.java** - Scan klasa (fizički operator)

---

## Pokretanje projekta

### Windows:
```batch
run.bat
```

### Linux/Mac:
```bash
./run.sh
```

### Manuelno:
```bash
mkdir -p out
find src/main/java -name "*.java" > sources.txt
javac -d out -encoding UTF-8 @sources.txt
cp src/main/resources/*.csv out/
java -cp out rs.raf.simpledb.MainQueryRunner
```

---

## Očekivani izlaz

Program prikazuje:
1. Optimizovani logički plan upita
2. Rezultate upita (predmeti sa brojem desetki)
3. Demonstraciju Block Nested Loop Join operatora

---

**Autor:** Student indeks 98  
**Predmet:** Napredne baze podataka / Baze podataka  
**Školska godina:** 2025/2026
