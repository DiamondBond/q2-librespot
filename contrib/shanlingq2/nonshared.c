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
