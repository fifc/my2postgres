#pragma once

#ifdef __cplusplus
extern "C" {
#endif 

#include <mysql.h>

int init();
int run();
int finish();

#ifdef __cplusplus
}
#endif