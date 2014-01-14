#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <CUnit/Basic.h>
#include <CUnit/TestRun.h>

extern CU_SuiteInfo cunit_suites[];

void AddTests() {
    assert(NULL != CU_get_registry());
    assert(!CU_is_test_running());

    if(CUE_SUCCESS != CU_register_suites(cunit_suites)){
        fprintf(stderr, "Register suites failed - %s ", CU_get_error_msg());
        exit(EXIT_FAILURE);
    }
}

int RunTest() {
    if(CU_initialize_registry()) {
        fprintf(stderr, " Initialization of Test Registry failed. ");
        exit(EXIT_FAILURE);
    }
    else {
        AddTests();
        /**** Automated Mode *****************
         * CU_set_output_filename("TestMax");
         * CU_list_tests_to_file();
         * CU_automated_run_tests();
         *//************************************/

        //***** Basice Mode *******************
        CU_basic_set_mode(CU_BRM_VERBOSE);
        CU_basic_run_tests();

        /*****Console Mode ********************
         * CU_console_run_tests();
         *//************************************/

        CU_cleanup_registry();

        return CU_get_error();
    }
}

int main(int argc, char * argv[]) {
    return RunTest();
}
