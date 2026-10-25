/* What glibc 2.28's libc_nonshared.a supplies and the Q2's rootfs lacks. libc.so.6 exports the stat
 * family only as __xstat64 and friends; _STAT_VER is 3 on MIPS o32 (stock binaries pass 3). */
struct stat64;
extern int __xstat64(int, const char *, struct stat64 *);
extern int __lxstat64(int, const char *, struct stat64 *);
extern int __fxstat64(int, int, struct stat64 *);
extern int __fxstatat64(int, int, const char *, struct stat64 *, int);

int stat64(const char *path, struct stat64 *buf) { return __xstat64(3, path, buf); }
int lstat64(const char *path, struct stat64 *buf) { return __lxstat64(3, path, buf); }
int fstat64(int fd, struct stat64 *buf) { return __fxstat64(3, fd, buf); }
int fstatat64(int dirfd, const char *path, struct stat64 *buf, int flags) {
    return __fxstatat64(3, dirfd, path, buf, flags);
}

/* libc.so.6 exports only __register_atfork */
extern void *__dso_handle; /* start.c */
extern int __register_atfork(void (*)(void), void (*)(void), void (*)(void), void *);
int pthread_atfork(void (*prepare)(void), void (*parent)(void), void (*child)(void)) {
    return __register_atfork(prepare, parent, child, __dso_handle);
}

/* glibc 2.34's 64-bit clock, which Rust's std calls instead of clock_gettime when it is linked (a
 * weak symbol). glibc 2.28's clock_gettime truncates seconds to 32 bits, so SystemTime, rustls'
 * certificate dates and the session's timestamps would wrap in January 2038. Linux 5.10 has
 * clock_gettime64, o32 syscall 4403; it fills a __kernel_timespec, two 64-bit words, whose
 * little-endian nanoseconds land where std's __timespec64 reads its 32-bit tv_nsec. */
extern int *__errno_location(void);
int __clock_gettime64(int clock, void *ts) {
    register long v0 __asm__("$2") = 4403;
    register long a0 __asm__("$4") = clock;
    register void *a1 __asm__("$5") = ts;
    register long a3 __asm__("$7");
    __asm__ volatile("syscall"
                     : "+r"(v0), "=r"(a3)
                     : "r"(a0), "r"(a1)
                     : "$1", "$3", "$8", "$9", "$10", "$11", "$12", "$13", "$14", "$15", "$24", "$25",
                       "hi", "lo", "memory");
    if (!a3) return 0;
    *__errno_location() = (int)v0;
    return -1;
}
