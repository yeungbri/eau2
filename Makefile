run:
	./eau2

m1:
	cd examples; g++ -std=c++17 -Wall -o m1 m1.cpp; ./m1

m2:
	cd examples; g++ -std=c++17 -Wall -o m2 m2.cpp; ./m2

m3:
	cd examples; g++ -std=c++17 -Wall -o m3 m3.cpp; ./m3

m4:
	cd examples; g++ -std=c++17 -Wall -o m4 m4.cpp; ./m4

m5:
	cd examples; g++ -std=c++17 -Wall -o m5 m5.cpp; ./m5

test:
	cd ./tests; cmake .; make dataframe_tests && ./dataframe_tests;
	cd ./tests; cmake .; make serialization_tests && ./serialization_tests;
	cd ./tests; cmake .; make kv_tests && ./kv_tests;

valgrind:
	make clean
	docker build -t memory-test:0.1 .
	#docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make dataframe_tests && valgrind --show-leak-kinds=all --leak-check=full ./dataframe_tests"
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make serialization_tests && valgrind --show-leak-kinds=all --leak-check=full ./serialization_tests"
	#docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; g++ -std=c++17 -Wall -g -o trivial_example trivial_example.cpp; valgrind --show-leak-kinds=all --leak-check=full ./trivial_example"

clean:
	rm -f tests/kv_tests
	rm -f tests/serialization_tests
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
	rm -f examples/m1
	rm -f examples/m2
	rm -f examples/m3
	rm -f examples/m4