using System;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Ultz.DF2
{
    public class Preface
    {
        public static readonly byte[] RawPreface =
        {
            // «DF2 10»\r\n\x1A\n
            // « - non-ASCII to prevent wrong file recognition and catches bad file transfers that clear bit 7
            // DF2 10 - the meaningful part of the preface. represents Data Format 2 v1.0
            // » - to match the first byte
            // \r\n - CRLF to catch bad file transfers that alter newline sequences
            // \x1A - Control-Z to prevent wrong file recognition on MS-DOS
            // \n - LF to catch bad file transfers that alter newline sequences
            0xAB, 0x44, 0x46, 0x32, 0x20, 0x31, 0x30, 0xBB, 0x0D, 0x0A, 0x1A, 0x0A
        };

        public bool IsValid(BinaryReader reader)
            => reader.ReadBytes(RawPreface.Length).SequenceEqual(RawPreface);
    }
}