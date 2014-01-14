#include "nettopology.h"

uint32_t net_get_distance(uint32_t ip1, uint32_t ip2) {
    uint32_t exact_dist;
    uint32_t std_dist;
    exact_dist = ip1^ip2;
    std_dist = 0;
    while(exact_dist) {
        exact_dist = exact_dist>>8;
        std_dist++;
    }
    return std_dist;
}


