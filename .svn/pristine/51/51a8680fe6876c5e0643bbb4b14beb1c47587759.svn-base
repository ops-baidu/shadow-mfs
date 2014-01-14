#ifndef _NETTOPOLOGY_H_
#define _NETTOPOLOGY_H_

#include <inttypes.h>

static inline uint32_t net_get_rack(uint32_t ip) {
    return ip>>8;
}

static inline int net_is_same_rack(uint32_t ip1, uint32_t ip2) {
    return net_get_rack(ip1) == net_get_rack(ip2);
}

uint32_t net_get_distance(uint32_t ip1, uint32_t ip2);

#endif
