run:
	./eau2

build:
	make clean
	# cd tests; g++ -std=c++17 -Wall -o basic_example basic_example.cpp; ./basic_example
	cd tests; g++ -std=c++17 -Wall -o trivial_example trivial_example.cpp; ./trivial_example

test:
	# make clean
	cd ./tests; cmake .; make dataframe_tests && ./dataframe_tests; make serialization_tests && ./serialization_tests;

valgrind:
	make clean
	docker build -t memory-test:0.1 .
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make dataframe_tests && valgrind --leak-check=full ./dataframe_tests"

clean:
	rm -f tests/serialization_tests
	rm -f tests/CMakeCache.txt
	rm -f tests/basic_example
	rm -rf tests/CMakeFiles/
	rm -rf tests/googletest-build/
	rm -rf tests/googletest-download/
	rm -rf tests/googletest-src/
	rm -rf tests/lib/
	rm -f tests/cmake_install.cmake
	rm -f tests/dataframe_tests 
	rm -f tests/Makefile
	rm -rf tests/bin