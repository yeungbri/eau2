build:
	g++ -std=c++17 -Wall -o eau2 src/main.cpp

run:
	./eau2

test:
	g++ -std=c++17 -Wall -o  test/personal_test_suite.cpp  