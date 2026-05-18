//go:build darwin

package server

/*
#include <mach/mach.h>

static uint64_t rss_bytes() {
    struct mach_task_basic_info info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) != KERN_SUCCESS) {
        return 0;
    }
    return (uint64_t)info.resident_size;
}
*/
import "C"

func processRSSBytes() uint64 {
	return uint64(C.rss_bytes())
}
