build:
	g++ -std=c++17 -Wall -o eau2 src/main.cpp

run:
	./eau2

test:
	g++ -std=c++17 -Wall -o tests/dataframe_tests tests/dataframe_tests.cpp