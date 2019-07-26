#include "de-error.h"
#include <stdio.h>
#include <stdlib.h>

void de_error(char *str){
  printf("DE Error: %s \n", str);
  exit(-1);
}

void de_message(char *str){
  printf("DE Message: %s \n", str);
}
