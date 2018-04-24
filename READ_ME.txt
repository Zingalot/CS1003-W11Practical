To compile:

mkdir -p bin
javac -cp "lib/*" src/*.java -d bin


To run:

Prior to running the  you run the program, make sure the specified output directory 
does NOT exist, by deleting it if need be.  

java -cp "lib/*:bin" WordCount input output 
