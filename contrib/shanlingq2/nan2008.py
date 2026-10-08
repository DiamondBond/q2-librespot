"""Set EF_MIPS_NAN2008 in a soft-float MIPS ELF's e_flags: the Q2's kernel won't exec it otherwise."""
import struct, sys
for p in sys.argv[1:]:
    b = bytearray(open(p, 'rb').read())
    assert b[:4] == b'\x7fELF' and b[0x12] == 8, p  # MIPS
    f, = struct.unpack_from('<I', b, 0x24)
    struct.pack_into('<I', b, 0x24, f | 0x400)
    open(p, 'wb').write(b)
