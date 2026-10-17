/* Entry point for linking against the Q2's glibc 2.28, whose rootfs ships no crt1.o: glibc's MIPS
 * __start (as Q2 Pod's helpers use it), handing __libc_start_main an init function that runs
 * .init_array, where Rust's std records argv. ld.so has already run .preinit_array, as for
 * glibc's own libc_nonshared __libc_csu_init. */
typedef void (*init_fn)(int, char **, char **);
extern init_fn __init_array_start[] __attribute__((weak)), __init_array_end[] __attribute__((weak));

void q2_csu_init(int argc, char **argv, char **envp) {
    for (init_fn *f = __init_array_start; f < __init_array_end; f++) (*f)(argc, argv, envp);
}

__asm__(".set noreorder\n.globl __start\n__start:\n"
        "lui $28, %hi(_gp)\naddiu $28, $28, %lo(_gp)\n"
        "move $31, $0\nlw $5, 0($29)\naddiu $6, $29, 4\n"
        "li $8, -8\nand $29, $29, $8\naddiu $29, $29, -32\n"
        "lui $4, %hi(main)\naddiu $4, $4, %lo(main)\n"
        "lui $7, %hi(q2_csu_init)\naddiu $7, $7, %lo(q2_csu_init)\n"
        "sw $0, 16($29)\nsw $2, 20($29)\nsw $29, 24($29)\n"
        "jal __libc_start_main\nnop\n1: b 1b\nnop\n.set reorder\n");

/* crt1.o's marker: without it glibc takes the program for a libc5-era one */
const int _IO_stdin_used = 0x20001;
/* crtbegin.o's: thread-local destructors register against it */
void *__dso_handle = &__dso_handle;
