build: main.cpp
	g++ -std=c++11 -O3 main.cpp -o sorer

test: sorer
	#Example from assignment description, retrun BOOL
	./sorer -f "test.sor" -from 0 -len 100 -print_col_type 0
	#Example from assignment description, retrun STRING
	./sorer -f "test.sor" -from 0 -len 100 -print_col_type 2
	#Exmaple from assignment description, return 0
	./sorer -f "test.sor" -from 0 -len 100 -is_missing_idx 2 0
	#Exmaple from assignment description, return 1
	./sorer -f "test.sor" -from 0 -len 100 -is_missing_idx 2 1
	#Exmaple from assignment description, return 1
	./sorer -f "test.sor" -from 0 -len 100 -is_missing_idx 2 2
	#Exmaple from assignment description, return hi
	./sorer -f "test.sor" -from 0 -len 100 -print_col_idx 2 0
	#Exmaple from assignment description, return 12
	./sorer -f "test.sor" -from 5 -len 100 -print_col_idx 1 0
	#Should return INT - tests that it identifies int over bool
	./sorer -f "test2.sor" -from 0 -len 200 -print_col_type 0 
	#Should return 1 - tests that first line is skipped and empty finding
	./sorer -f "test2.sor" -from 5 -len 150 -is_missing_idx 3 1
	#Should return STRING - test space inside quotation
	./sorer -f "test2.sor" -from 0 -len 200 -print_col_type 6
	#Should return 1 - test invalid field as missing idx
	./sorer -f "test2.sor" -from 0 -len 200 -is_missing_idx 1 2
	#Should return hey - test leading and trailing space
	./sorer -f "test2.sor" -from 0 -len 200 -print_col_idx 1 0
	#Should return FLOAT - test float over int
	./sorer -f "test2.sor" -from 0 -len 200 -print_col_type 3

clean:
	rm sorer
