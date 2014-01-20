#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <CUnit/CUnit.h>
#include <CUnit/Automated.h>
#include <CUnit/TestDB.h>
#include "main.h"
#include "matocsserv.h"
#include "chunks.h"
#include "nettopology.h"
static serventry *matocsservhead = NULL;

//check the replication is multirack
int check_replist_rack(void* ptrs[], int repnum) {
    int i,multirack;
    serventry *eptr, *eptr_first;
    eptr_first = (serventry *)ptrs[0];
    multirack = 0;
    for(i=0; i<repnum; i++) {
        eptr = (serventry *)ptrs[i];
        if(!net_is_same_rack(eptr->servip, eptr_first->servip)){
            return 1;
        }
    }
    return 0;
}

//check the replication is unique from each other
int check_replist_unique(void* ptr[], int repnum) {
    int i,j;
    for(i=0; i<repnum-1; i++) {
        for(j=i+1; j<repnum; j++) {
            if(((serventry *)ptr[i])->servip == ((serventry *)ptr[j])->servip) {
                return 0;
            }
        }
    }
    return 1;
}

//check the chunk is need move from high usage chunkserver to low usage chunkserver
int check_chunk_need_move(void* ptr[], int repnum, double limit) {
    int i;
    for(i=0; i<repnum; i++) {
        if((double)((serventry *)ptr[i])->usedspace / (double)((serventry *)ptr[i])->totalspace > limit) {
            return 1;
        }
    }
    return 0;
}

int check_rep_resource(void *ptr) {
    serventry *eptr = (serventry *)ptr;
    uint64_t lowfree = 100;
    lowfree = lowfree<<30;
    lowfree = lowfree > eptr->totalspace/10 ? lowfree : eptr->totalspace/10;
    if(eptr->totalspace - eptr->usedspace > lowfree) {
        return 1;
    }
    return 0;
}

void print_chunkserver_info(void *ptr) {
    serventry *eptr = (serventry *)ptr;
    printf("chunkserver info: ip:%s, used:%lu, total:%lu.\n", eptr->servstrip, eptr->usedspace>>30, eptr->totalspace>>30);
}

        
void test_matocsserv_getservers_samerack() {
    void* ptrs[65536];
    uint16_t servcount;
    int64_t i,j,select_index;
    uint64_t limitspace = 1800;
    int select_count[5] = {0};
    limitspace = limitspace<<30;
    //select 1000 times, test result.
    for(i=0; i<1000; i++) {
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        CU_ASSERT_TRUE(servcount==3);
        CU_ASSERT_TRUE(check_replist_unique(ptrs, 3));
        for(j=0; j<3; j++) {
            CU_ASSERT_TRUE(check_rep_resource(ptrs[j]));
            select_index = (((serventry *)ptrs[j])->totalspace>>30) / 2000;
            select_count[select_index]++;
        }
    }
    for(i=0; i<4; i++) {
        //printf("select_count index:%lu, count:%d.\n", i, select_count[i]);
        CU_ASSERT_TRUE(select_count[i] < select_count[i+1]);
    }
    //printf("select_count index:%lu, count:%d.\n", 4, select_count[i]);

}

void test_matocsserv_getservers_multirack() {
    void* ptrs[65536];
    uint64_t i,j,select_index;
    uint64_t limitspace;
    int select_count[5] = {0};

    uint16_t servcount;
   
    limitspace = 400;
    limitspace = limitspace<<30;
    //select 1000 times, and test result
    for(i=0; i<1000; i++) {
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        CU_ASSERT_TRUE(servcount==3);
        CU_ASSERT_TRUE(check_replist_unique(ptrs, 3));
        CU_ASSERT_TRUE(check_replist_rack(ptrs, 3));
        for(j=0; j<3; j++) {
            CU_ASSERT_TRUE(check_rep_resource(ptrs[j]));
            select_index = (((serventry *)ptrs[j])->totalspace>>30) / 500;
            select_count[select_index]++;
        }
    }
    for(i=0; i<4; i++) {
        //printf("select_count index:%lu, count:%d\n", i, select_count[i]);
        CU_ASSERT_TRUE(select_count[i] < select_count[i+1]);
    }
    //printf("select_count index:%lu, count:%d.\n", 4, select_count[i]);
}

//pick servers in samerack, then delete one copy
void test_chunk_delete_samerack() {
    void* ptrs[65536];
    uint16_t servcount;
    uint64_t i;
    void *chunk_ptr, *serv_ptr;

    for(i=0; i<1000; i++) {
        servcount = matocsserv_getservers_wrandom(ptrs, 4);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 4, 3);
        serv_ptr = chunk_unittest_delete_overgoal(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
    }
}

void test_chunk_delete_multirack() {
    void* ptrs[65536];
    uint16_t servcount;
    uint64_t i, j;
    void *chunk_ptr, *serv_ptr;
    
    //create 1000 chunks, then delete one copy
    for(i=0; i<1000; i++) {
        //select 4 copys, then delete one.
        servcount = matocsserv_getservers_wrandom(ptrs, 4);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 4, 3);
        serv_ptr = chunk_unittest_delete_overgoal(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
        //remove the copy selected, then test rack
        for(j=0; j<3; j++){
            if(serv_ptr == ptrs[j]) {
                ptrs[j] = ptrs[3];
            }
        }
        CU_ASSERT_TRUE(check_replist_rack(ptrs, 3));

        //select 2 copys, then delete one.
        servcount = matocsserv_getservers_wrandom(ptrs, 2);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 2, 1);
        serv_ptr = chunk_unittest_delete_overgoal(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
    }
}

void test_chunk_recovery_samerack() {
    void* ptrs[65536];
    uint64_t i;
    uint16_t servcount;
    void *chunk_ptr, *serv_ptr;

    //create 1000 chunks whose replication number less than goal, then recovery
    for(i=0; i<1000; i++) {
        //select 3 copys, then recovery the chunk.
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        //use front two ptr to create chunk, like a chunk lost a copy
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 2, 3);
        serv_ptr = chunk_unittest_recovery_select(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
        ptrs[2] = serv_ptr;
    }
}

void test_chunk_recovery_multirack() {
    void* ptrs[65536];
    uint64_t i;
    uint16_t servcount;
    void *chunk_ptr, *serv_ptr;
    serventry *eptr;

    //create 1000 chunks whose replication number less than goal, then recovery
    for(i=0; i<1000; ) {
        //select 3 copys, then recovery the chunk.
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        //use front two ptr to create chunk, like a chunk lost a copy
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 2, 3);
        //if the two copy is in same rack, then make a count
        if(!check_replist_rack(ptrs, 2)) {
            i++;
        }
        serv_ptr = chunk_unittest_recovery_select(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
        ptrs[2] = serv_ptr;
        CU_ASSERT_TRUE(check_replist_rack(ptrs, 3));
    }
    
    for(i=0; i<1000; i++) {
        //select one copy for chunk, then recovery it
        servcount = matocsserv_getservers_wrandom(ptrs, 1);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 1, 3);
        serv_ptr = chunk_unittest_recovery_select(chunk_ptr);
        CU_ASSERT_TRUE(serv_ptr!=NULL);
        ptrs[1] = serv_ptr;
        CU_ASSERT_TRUE(check_replist_rack(ptrs, 2));
    }

    //make 128,256 rack's chunkserver is over write rep limit, then recovery
    for(eptr = matocsservhead; eptr != NULL; eptr = eptr->next) {
        if(net_get_rack(eptr->servip) == 1) {
            continue;
        }
        eptr->wrepcounter = 10;
    }
    for(i=0; i<1000; ) {
        //select 3 copys, then recovery the chunk.
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        //use front two ptr to create chunk, like a chunk lost a copy
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 2, 3);
        //if the two copy is in same rack, then make a count
        if(!check_replist_rack(ptrs, 2)) {
            i++;
        }
        serv_ptr = chunk_unittest_recovery_select(chunk_ptr);
        if (check_replist_rack(ptrs, 2) || net_get_rack(((serventry*)ptrs[0])->servip) != 1) {
            CU_ASSERT_TRUE(serv_ptr!=NULL);
        }
        else {
            CU_ASSERT_TRUE(serv_ptr==NULL);
        }
    }
    //make the wrepcounter to 0, the env will be used for other cases.
    for(eptr = matocsservhead; eptr != NULL; eptr = eptr->next) {
        eptr->wrepcounter = 0;
    }
    
}

void test_chunk_move_samerack() {
    void* ptrs[65536];
    uint64_t i, j;
    uint16_t servcount;
    void *chunk_ptr;
    void *ptr_src, *ptr_dst;
    int move_result, move_check;

    //create 1000 chunks, then to move for cluster balance.
    for(i=0; i<1000; ) {
        //select 3 copys, then test move the chunk.
        servcount = matocsserv_getservers_wrandom(ptrs, 3);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 3, 3);
        move_result = chunk_unittest_balance(chunk_ptr, &ptr_src, &ptr_dst);
        move_check = check_chunk_need_move(ptrs, 3, 0.48);
        if(move_check) {
            CU_ASSERT_TRUE(move_result);
            for(j=0; j<3; j++) {
                if(ptrs[j] == ptr_src) {
                    break;
                }
            }
            CU_ASSERT_TRUE(ptrs[j]==ptr_src);
            CU_ASSERT_TRUE(ptr_src!=ptr_dst);
            i++;
        }
        else {
            CU_ASSERT_TRUE(!move_result);
        }
    }

    for(i=0; i<1000; ) {
        //select one copy for chunk, then move it
        servcount = matocsserv_getservers_wrandom(ptrs, 1);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 1, 1);
        move_result = chunk_unittest_balance(chunk_ptr, &ptr_src, &ptr_dst);
        move_check = check_chunk_need_move(ptrs, 1, 0.48);
        if(move_check) {
            CU_ASSERT_TRUE(move_result);
            CU_ASSERT_TRUE(ptrs[0]==ptr_src);
            CU_ASSERT_TRUE(ptr_src!=ptr_dst);
            i++;
        }
        else {
            CU_ASSERT_TRUE(!move_result);
        }
    }
}

void test_chunk_move_multirack() {
    void* ptrs[65536];
    uint64_t i, j, copys;
    uint16_t servcount;
    void *chunk_ptr;
    void *ptr_src, *ptr_dst;
    int move_result, move_check;

    for(copys=2; copys<=4; copys++) {
        //create 1000 chunks, then to move for cluster balance.
        for(i=0; i<1000; ) {
            //select 3 copys, then test move the chunk.
            servcount = matocsserv_getservers_wrandom(ptrs, copys);
            chunk_ptr = chunk_unittest_create_by_servlist(ptrs, copys, copys);
            move_result = chunk_unittest_balance(chunk_ptr, &ptr_src, &ptr_dst);
            move_check = check_chunk_need_move(ptrs, copys, 0.455);
            if(move_check) {
                //for(j=0;j<3;j++) {
                //    printf("chunk copy %lu, ip:%s\n", j, ((serventry *)ptrs[j])->servstrip);
                //}
                //printf("chunk move src, ip:%s\n", ((serventry *)ptr_src)->servstrip);
                //printf("chunk move dst, ip:%s\n", ((serventry *)ptr_dst)->servstrip);
                CU_ASSERT_TRUE(move_result);
                for(j=0; j<copys; j++) {
                    if(ptrs[j] == ptr_src) {
                        break;
                    }
                }
                CU_ASSERT_TRUE(ptrs[j]==ptr_src);
                CU_ASSERT_TRUE(ptr_src!=ptr_dst);
                ptrs[j] = ptr_dst;
                CU_ASSERT_TRUE(check_replist_rack(ptrs, copys));
                i++;
            }
            else {
                CU_ASSERT_TRUE(!move_result);
            }
        }
    }

    for(i=0; i<1000; ) {
        //select one copy for chunk, then move it
        servcount = matocsserv_getservers_wrandom(ptrs, 1);
        chunk_ptr = chunk_unittest_create_by_servlist(ptrs, 1, 1);
        move_result = chunk_unittest_balance(chunk_ptr, &ptr_src, &ptr_dst);
        move_check = check_chunk_need_move(ptrs, 1, 0.455);
        if(move_check) {
            CU_ASSERT_TRUE(move_result);
            CU_ASSERT_TRUE(ptrs[0]==ptr_src);
            CU_ASSERT_TRUE(ptr_src!=ptr_dst);
            i++;
        }
        else {
            CU_ASSERT_TRUE(!move_result);
        }
    }
}

CU_TestInfo samerack_cases[] = {
    {"getservers in same rack:", test_matocsserv_getservers_samerack},
    {"chunk delete in same rack:", test_chunk_delete_samerack},
    {"chunk recovery in same rack:", test_chunk_recovery_samerack},
    {"chunk move in same rack:", test_chunk_move_samerack},
    CU_TEST_INFO_NULL
};


CU_TestInfo multirack_cases[] = {
    {"getservers in multirack:", test_matocsserv_getservers_multirack},
    {"chunk delete in multirack:", test_chunk_delete_multirack},
    {"chunk recovery in multirack:", test_chunk_recovery_multirack},
    {"chunk move in multirack:", test_chunk_move_multirack},
    CU_TEST_INFO_NULL
};

//add chunkserver in same rack, some of the chunkservers in don't have enough resource
int suite_samerack_init(void) {
    uint32_t rackid = 128;
    uint64_t i,j;

    matocsserv_unittest_init(100, 0.9);
    chunk_uinttest_init(5, 1);

    uint64_t usedspace, totalspace;
    for(i=0; i<20; i++) {
        usedspace = i*100;
        usedspace = usedspace << 30;
        totalspace = 2000;
        totalspace = totalspace << 30;

        for(j=0; j<5; j++) {
            matocsservhead = matocsserv_unittest_add_chunkserver_batch((rackid<<8) + i*10 + j*2, usedspace*j, totalspace*j, 2);
        }
    }
    matocsserv_status();

    return 0;
}

//add chunkserver in multi rack, number in first rack greater than the second
int suite_multirack_init(void) {
    uint64_t i,j;
    uint32_t rackid[3] = {1, 128, 256};

    matocsserv_unittest_init(100, 0.9);
    chunk_uinttest_init(5, 1);

    uint64_t usedspace, totalspace;
    for(i=0; i<10; i++) {
        usedspace = i*50;
        usedspace = usedspace << 30;
        totalspace = 500;
        totalspace = totalspace << 30;


        for(j=0; j<5; j++) {
            matocsservhead = matocsserv_unittest_add_chunkserver_batch((rackid[0]<<8) + i*20 + j*4, usedspace*j, totalspace*j, 4);
            matocsservhead = matocsserv_unittest_add_chunkserver_batch((rackid[1]<<8) + i*5 + j*1, usedspace*j, totalspace*j, 1);
            matocsservhead = matocsserv_unittest_add_chunkserver_batch((rackid[2]<<8) + i*5 + j*1, usedspace*j, totalspace*j, 1);
        }
    }
    matocsserv_status();
    return 0;
}

int suite_getservers_clean(void) {
    matocsserv_unittest_clean();
    return 0;
}

CU_SuiteInfo cunit_suites[] = {
    {"chunkserver in samerack.", suite_samerack_init, suite_getservers_clean, samerack_cases},
    {"chunkserver in multirack.:", suite_multirack_init, suite_getservers_clean, multirack_cases},
    CU_SUITE_INFO_NULL
};

