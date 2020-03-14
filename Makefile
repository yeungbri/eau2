build:
	g++ -std=c++17 -Wall -o eau2 src/main.cpp

run:
	./eau2

test:
	cd ./tests; cmake .; make dataframe_tests && ./dataframe_tests

valgrind:
	docker build -t memory-test:0.1 .
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make dataframe_tests && valgrind --leak-check=full ./dataframe_tests"

clean:
	rm -f tests/CMakeCache.txt
	rm -rf tests/CMakeFiles/
	rm -rf tests/googletest-build/
	rm -rf tests/googletest-download/
	rm -rf tests/googletest-src/
	rm -rf tests/lib/
	rm -f tests/cmake_install.cmake
	rm -f tests/dataframe_tests 
	rm -f tests/Makefile
	rm -rf tests/bin