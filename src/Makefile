all:
	javac -cp postgresql-42.0.0.jar:. DCube.java
	java -cp postgresql-42.0.0.jar:. DCube $(PWD)/airforce_cnt.csv output 7 ari density 5
	#java -cp postgresql-42.0.0.jar:. DCube $(PWD)/darpa.csv output 3 ari density 3

clean:
	rm -f *.class
