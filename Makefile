milestones:
	cd examples; g++ -std=c++17 -Wall -o milestones milestones.cpp; ./milestones

test:
	cd ./tests; cmake .; make dataframe_tests && ./dataframe_tests;
	cd ./tests; cmake .; make serialization_tests && ./serialization_tests;
	cd ./tests; cmake .; make kv_tests && ./kv_tests;

valgrind:
	docker build -t memory-test:0.1 .
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make dataframe_tests && valgrind ./dataframe_tests; make serialization_tests && valgrind ./serialization_tests; make kv_tests && valgrind ./kv_tests"

valgrind-milestones:
	docker build -t memory-test:0.1 .
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/examples; g++ -pthread -std=c++17 -Wall -o milestones milestones.cpp; valgrind ./milestones"

valgrind-verbose:
	docker build -t memory-test:0.1 .
	docker run -ti -v "$$(pwd)":/test memory-test:0.1 bash -c "cd ./test/tests; cmake .; make dataframe_tests && valgrind --show-leak-kinds=all --leak-check=full --track-origins=yes ./dataframe_tests; make serialization_tests && valgrind --show-leak-kinds=all --leak-check=full --track-origins=yes ./serialization_tests; make kv_tests && valgrind --show-leak-kinds=all --leak-check=full --track-origins=yes ./kv_tests; cd ../examples; g++ -std=c++17 -Wall -o milestones milestones.cpp; valgrind --show-leak-kinds=all --leak-check=full --track-origins=yes ./milestones"

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
	rm -f examples/milestones