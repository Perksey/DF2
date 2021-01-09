using System;

namespace Ultz.DF2
{
    [Flags]
    public enum StreamMode
    {
        Read = 1,
        Write = 1 << 1,
        ReadWrite = Read | Write
    }
}