#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>
#include<signal.h>
#include<stdio.h>


void print_stacktrace(FILE *out, unsigned int max_frames);

void sig_handler(int);

#define INSTALL_SIGSEGV_TRAP            \
struct Guard                            \
{                                       \
    Guard()                             \
    {                                   \
        signal(SIGSEGV, sig_handler);   \
    }                                   \
};                                      \
static Guard g = Guard();
