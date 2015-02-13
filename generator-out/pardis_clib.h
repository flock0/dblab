#ifndef __PARDIS_CLIB_H__
	#define __PARDIS_CLIB_H__

// Domain specific libraries should be put here and not in CCodeGenerator of pardis
#include <glib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>

unsigned long long timeval_subtract(struct timeval *result, struct timeval *t2, struct timeval *t1) {
   	int diff = (t2->tv_usec + 1000000 * t2->tv_sec) - (t1->tv_usec + 1000000 * t1->tv_sec);
	result->tv_sec = diff / 1000000;
	result->tv_usec = diff % 1000000;
    return ((result->tv_sec * 1000) + (result->tv_usec/1000));
}

char* ltoa(long num) {
	const int n = snprintf(NULL, 0, "%lu", num);
	char* buf = (char*)malloc((n+1)*sizeof(char));
	snprintf(buf, n+1, "%lu", num);
	return buf;
} 

// These can be moved to the transformer for mmap
char* strntoi_unchecked(char *s, int* num) {
	int n = 0;
	int sign = 1;
	if (*s == '-') {
		s++;
		sign = -1;
	}
	while (*s != '|' && *s != '\n' && *s != '-' /* for dates */) {
		n *= 10;
		n += *s++ - '0';
	}
	s++; // skip '|' 

	*num = sign * n;
	return s;
}

static const double fractions[] = {
	1.0e-1,  1.0e-2,  1.0e-3,  1.0e-4,  1.0e-5,
	1.0e-6,  1.0e-7,  1.0e-8,  1.0e-9,  1.0e-10,
	1.0e-11, 1.0e-12, 1.0e-13, 1.0e-14, 1.0e-15,
	1.0e-16, 1.0e-17, 1.0e-18, 1.0e-19
};

char* strntod_unchecked(char *s, double* num)
{
	int n = 0;
	int d = 0;
	int dlen = 0;
	int sign = 1;

	if (*s == '-') {
		s++;
		sign = -1;
	}

	while (*s != '|' && *s != '\n' && *s != '.') {
		n *= 10;
		n += *s++ - '0';
	}

	if (*s != '|' && *s != '\n') {
		s++;
		while (*s != '|' && *s != '\n') {
			d *= 10;
			d += *s++ - '0';
			dlen++;
		}
	}

	s++; // skip '|'

	*num = sign * (n + d * fractions[dlen - 1]);
	return s;
}

#define pointer_assign(x, y) (*x = y)

#define pointer_add(x, y) (x + y)

#define str_subtract(x, y) (x - y)

typedef GHashTable LGHashTable;
typedef GList LGList;

#endif
