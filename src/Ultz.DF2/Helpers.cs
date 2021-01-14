using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ultz.DF2
{
    public static class Helpers
    {
        internal static string CoreToString(object obj) => obj is IEnumerable enumerable && obj is not string
            ? "[(" + string.Join("), (", enumerable.Cast<object>().Select(CoreToString)) + ")]"
            : obj?.ToString() ?? "<NULL>";

        public static string ReadDf2String(this BinaryReader reader)
        {
            return Encoding.UTF8.GetString(CoreRead(reader).ToArray());
            
            static IEnumerable<byte> CoreRead(BinaryReader reader)
            {
                while (true)
                {
                    var b = reader.ReadByte();
                    if (b == 0x00)
                    {
                        yield break;
                    }

                    yield return b;
                }
            }
        }

        public static IValue? GetValue(this IGroup @this, string absolutePath)
        {
            absolutePath = absolutePath.TrimEnd('/');
            IValue? ret = null;
            foreach (var element in absolutePath.Split(new []{'/'}, StringSplitOptions.RemoveEmptyEntries))
            {
                if (ret is null)
                {
                    ret = @this.Values[element];
                }
                else
                {
                    if (ret is Group group)
                    {
                        ret = group.Values[element];
                    }
                    else
                    {
                        throw new InvalidOperationException("Attempted to read within a value instead of a group.");
                    }
                }
            }

            return ret;
        }

        public static void WriteDf2String(this BinaryWriter writer, string value)
        {
            writer.Write(Encoding.UTF8.GetBytes(value));
            writer.Write((byte)0x00);
        }

        private static void AssertValue(IValue o)
        {
            if (o is not Value)
            {
                throw new ArgumentException($"{o.GetType().Name} is not a Value");
            }
        }

        private static void AssertKind(ValueKind left, ValueKind right)
        {
            if (left != right)
            {
                throw new DataException($"Attempted to interpret a {left} as a {right}");
            }
        }
        
        public static byte AsByte(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Byte);
            return (byte)((Value)value).Data;
        }
        public static sbyte AsSByte(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.SByte);
            return (sbyte)((Value)value).Data;
        }
        public static short AsShort(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Short);
            return (short)((Value)value).Data;
        }
        public static ushort AsUShort(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.UShort);
            return (ushort)((Value)value).Data;
        }
        public static int AsInt(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Int);
            return (int)((Value)value).Data;
        }
        public static uint AsUInt(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.UInt);
            return (uint)((Value)value).Data;
        }
        public static long AsLong(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Long);
            return (long)((Value)value).Data;
        }
        public static ulong AsULong(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.ULong);
            return (ulong)((Value)value).Data;
        }
        public static float AsFloat(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Float);
            return (float)((Value)value).Data;
        }
        public static double AsDouble(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Double);
            return (double)((Value)value).Data;
        }
        public static string AsString(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.String);
            return (string)((Value)value).Data;
        }
        public static Group AsGroup(this IValue value)
        {
            return (Group) value;
        }
        public static T[] AsArray<T>(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.Array);
            return (T[])((Value)value).Data;
        }
        public static IEnumerable AsList(this IValue value)
        {
            AssertValue(value);
            AssertKind(value.Kind, ValueKind.List);
            return (IEnumerable)((Value)value).Data;
        }

        public static object GetValue(this IValue value)
        {
            AssertValue(value);
            return ((Value) value).Data;
        }

        public static void WriteDf2Int(this BinaryWriter writer, int value)
            => writer.WriteDf2UInt(Unsafe.As<int, uint>(ref value));

        public static void WriteDf2UInt(this BinaryWriter writer, uint uValue)
        {
            // Write out an int 7 bits at a time. The high bit of the byte,
            // when on, tells reader to continue reading more bytes.
            //
            // Using the constants 0x7F and ~0x7F below offers smaller
            // codegen than using the constant 0x80.
 
            while (uValue > 0x7Fu)
            {
                writer.Write((byte)(uValue | ~0x7Fu));
                uValue >>= 7;
            }
 
            writer.Write((byte)uValue);
        }

        public static void WriteDf2Int64(this BinaryWriter writer, long value)
            => writer.WriteDf2UInt64(Unsafe.As<long, ulong>(ref value));
 
        public static void WriteDf2UInt64(this BinaryWriter writer, ulong uValue)
        {
            // Write out an int 7 bits at a time. The high bit of the byte,
            // when on, tells reader to continue reading more bytes.
            //
            // Using the constants 0x7F and ~0x7F below offers smaller
            // codegen than using the constant 0x80.
 
            while (uValue > 0x7Fu)
            {
                writer.Write((byte)((uint)uValue | ~0x7Fu));
                uValue >>= 7;
            }
 
            writer.Write((byte)uValue);
        }

        public static int ReadDf2Int(this BinaryReader reader)
            => Unsafe.As<uint, int>(ref Unsafe.AsRef(reader.ReadDf2UInt()));
        public static uint ReadDf2UInt(this BinaryReader reader)
        {
            // Unlike writing, we can't delegate to the 64-bit read on
            // 64-bit platforms. The reason for this is that we want to
            // stop consuming bytes if we encounter an integer overflow.
 
            uint result = 0;
            byte byteReadJustNow;
 
            // Read the integer 7 bits at a time. The high bit
            // of the byte when on means to continue reading more bytes.
            //
            // There are two failure cases: we've read more than 5 bytes,
            // or the fifth byte is about to cause integer overflow.
            // This means that we can read the first 4 bytes without
            // worrying about integer overflow.
 
            const int maxBytesWithoutOverflow = 4;
            for (var shift = 0; shift < maxBytesWithoutOverflow * 7; shift += 7)
            {
                // ReadByte handles end of stream cases for us.
                byteReadJustNow = reader.ReadByte();
                result |= (byteReadJustNow & 0x7Fu) << shift;
 
                if (byteReadJustNow <= 0x7Fu)
                {
                    return result; // early exit
                }
            }
 
            // Read the 5th byte. Since we already read 28 bits,
            // the value of this byte must fit within 4 bits (32 - 28),
            // and it must not have the high bit set.
 
            byteReadJustNow = reader.ReadByte();
            if (byteReadJustNow > 0b_1111u)
            {
                throw new FormatException("Bad 7-bit int");
            }
 
            result |= (uint)byteReadJustNow << (maxBytesWithoutOverflow * 7);
            return result;
        }
 
        public static long ReadDf2Int64(this BinaryReader reader)
            => Unsafe.As<ulong, long>(ref Unsafe.AsRef(reader.ReadDf2UInt64()));
        public static ulong ReadDf2UInt64(this BinaryReader reader)
        {
            ulong result = 0;
            byte byteReadJustNow;
 
            // Read the integer 7 bits at a time. The high bit
            // of the byte when on means to continue reading more bytes.
            //
            // There are two failure cases: we've read more than 10 bytes,
            // or the tenth byte is about to cause integer overflow.
            // This means that we can read the first 9 bytes without
            // worrying about integer overflow.
 
            const int maxBytesWithoutOverflow = 9;
            for (var shift = 0; shift < maxBytesWithoutOverflow * 7; shift += 7)
            {
                // ReadByte handles end of stream cases for us.
                byteReadJustNow = reader.ReadByte();
                result |= (byteReadJustNow & 0x7Ful) << shift;
 
                if (byteReadJustNow <= 0x7Fu)
                {
                    return result; // early exit
                }
            }
 
            // Read the 10th byte. Since we already read 63 bits,
            // the value of this byte must fit within 1 bit (64 - 63),
            // and it must not have the high bit set.
 
            byteReadJustNow = reader.ReadByte();
            if (byteReadJustNow > 0b_1u)
            {
                throw new FormatException("Bad 7-bit int");
            }
 
            result |= (ulong)byteReadJustNow << (maxBytesWithoutOverflow * 7);
            return result;
        }
    }
}