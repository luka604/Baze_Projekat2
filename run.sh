#!/bin/bash
echo "==================================="
echo "RAF SimpleDB - Kolokvijum Runner"
echo "==================================="
echo

# Kompajliranje ako nema out folder
if [ ! -d "out" ]; then
    echo "Kompajliranje projekta..."
    mkdir -p out
    find src/main/java -name "*.java" > sources.txt
    javac -d out -encoding UTF-8 @sources.txt
    cp src/main/resources/*.csv out/
    rm sources.txt
    echo "Kompajliranje završeno."
    echo
fi

# Brisanje stare baze ako postoji (opcionalno)
if [ -d "studentdb" ]; then
    echo "Brisanje stare baze podataka..."
    rm -rf studentdb
    echo
fi

echo "Pokretanje programa..."
echo
java -cp out rs.raf.simpledb.MainQueryRunner
