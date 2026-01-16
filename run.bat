@echo off
echo ===================================
echo RAF SimpleDB - Kolokvijum Runner
echo ===================================
echo.

REM Kompajliranje ako nema out folder
if not exist out (
    echo Kompajliranje projekta...
    mkdir out
    dir /s /b src\main\java\*.java > sources.txt
    javac -d out -encoding UTF-8 @sources.txt
    copy src\main\resources\*.csv out\
    del sources.txt
    echo Kompajliranje zavrseno.
    echo.
)

REM Brisanje stare baze ako postoji (opcionalno)
if exist studentdb (
    echo Brisanje stare baze podataka...
    rmdir /s /q studentdb
    echo.
)

echo Pokretanje programa...
echo.
java -cp out rs.raf.simpledb.MainQueryRunner

echo.
pause
