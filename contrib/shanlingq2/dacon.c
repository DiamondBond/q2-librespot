/* Power the Q2's CS43131 headphone DAC on, as q2-rockbox's shanlingq2_codec.c
 * does. demo's check_dacoff_state powers it off after about 30 s with nothing
 * of its own playing, so onevent.sh runs this when librespot starts playing.
 *   dacon [volume 0..100, 100 = 0 dB] */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <unistd.h>

static void set(int fd, unsigned long req, int v)
{
    if (ioctl(fd, req, &v) < 0)
        perror("dacon: ioctl");
}

int main(int argc, char **argv)
{
    int v = argc > 1 ? atoi(argv[1]) : 70;
    int fd = open("/dev/shanling_dac", O_RDWR);
    if (fd < 0) {
        perror("dacon: /dev/shanling_dac");
        return 1;
    }
    set(fd, 0xc0044d1c, 1);          /* headset output */
    set(fd, 0xc0044d1a, 1);          /* power */
    set(fd, 0xc0044d1b, 0);          /* PCM, not DSD */
    set(fd, 0xc0044d00, v << 8 | v); /* volume */
    set(fd, 0xc0044d1f, 0);          /* unmute */
    close(fd);
    return 0;
}
