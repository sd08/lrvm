/* truncate the log; manually inspect to see that the log has shrunk
 * to nothing */

#include "rvm.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#define TEST_STRING "hello, world"
#define TEST_STRING2 "HELLO, WORLD"
#define OFFSET2 1000


/* proc1 writes some data, commits it, then exits */
void proc1(rvm_t rvm) 
{

     trans_t trans;
     char* segs[1];
     
     rvm_destroy(rvm, "testseg");
     segs[0] = (char *) rvm_map(rvm, "testseg", 10000);

     
     trans = rvm_begin_trans(rvm, 1, (void **) segs);
     
     rvm_about_to_modify(trans, segs[0], 0, 100);
     sprintf(segs[0], TEST_STRING);
     
     rvm_about_to_modify(trans, segs[0], OFFSET2, 100);
     sprintf(segs[0]+OFFSET2, TEST_STRING);
     
     rvm_commit_trans(trans);

     trans = rvm_begin_trans(rvm, 1, (void **) segs);
     
     rvm_about_to_modify(trans, segs[0], 0, 100);
     sprintf(segs[0], TEST_STRING2);
     
     rvm_about_to_modify(trans, segs[0], OFFSET2, 100);
     sprintf(segs[0]+OFFSET2, TEST_STRING2);
     
     rvm_commit_trans(trans);

}

int main(int argc, char **argv) 
{
     rvm_t rvm;
     
	rvm = rvm_init("rvm_segments");
	proc1(rvm);
	printf("Before Truncation:\n");
	system("ls -l rvm_segments");
	system("hexdump -C rvm_segments/testseg.rvm");
	rvm_truncate_log(rvm);

	printf("\nAfter Truncation:\n");
	system("ls -l rvm_segments");
	system("hexdump -C rvm_segments/testseg.rvm");

	return 0;
}
