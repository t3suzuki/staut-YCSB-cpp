#include <stdlib.h>
#include <sys/mman.h>

int
main()
{
  size_t length = 20ULL * 1024 * 1024 * 1024;
  char *addr = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
  if (addr == MAP_FAILED)
    exit(EXIT_FAILURE);  
}
