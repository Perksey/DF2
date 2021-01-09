using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace Ultz.DF2
{
    public static class Helpers
    {
        public static Span<byte> WriteBytes(Span<byte> bytes, uint value)
        {
            if (value < 0x80)
            {
                bytes[0] = ((byte) value);
                return bytes.Slice(0, 1);
            }
            else if (value < 0x4000)
            {
                bytes[0] = ((byte) (0x80 | (value >> 8)));
                bytes[1] = ((byte) (value & 0xff));
                return bytes.Slice(0, 2);
            }
            else
            {
                bytes[0] = ((byte) ((value >> 24) | 0xc0));
                bytes[1] = ((byte) ((value >> 16) & 0xff));
                bytes[2] = ((byte) ((value >> 8) & 0xff));
                bytes[3] = ((byte) (value & 0xff));
                return bytes.Slice(0, 4);
            }
        }

        public static Span<byte> WriteBytes(Span<byte> bytes, int value)
        {
            if (value >= 0)
            {
                return WriteBytes(bytes, (uint) (value << 1));
            }

            if (value > -0x40)
            {
                value = 0x40 + value;
            }
            else if (value >= -0x2000)
            {
                value = 0x2000 + value;
            }
            else if (value >= -0x20000000)
            {
                value = 0x20000000 + value;
            }

            return WriteBytes(bytes, (uint) ((value << 1) | 1));
        }

        public static void ReadBytes(Span<byte> bytes, out uint value)
        {
            var i = 0;
            var first = bytes[i++];
            if ((first & 0x80) == 0)
                value = first;

            if ((first & 0x40) == 0)
                value = ((uint) (first & ~0x80) << 8)
                       | bytes[i++];

            value = ((uint) (first & ~0xc0) << 24)
                   | (uint) bytes[i++] << 16
                   | (uint) bytes[i++] << 8
                   | bytes[i++];
        }

        public static void ReadBytes(Span<byte> bytes, out int value)
        {
            var b = bytes [0];
            ReadBytes (bytes, out uint ut);
            var u = (int) ut;
            var v = u >> 1;
            if ((u & 1) == 0)
                value = v;

            switch (b & 0xc0)
            {
                case 0:
                case 0x40:
                    value = v - 0x40;
                    break;
                case 0x80:
                    value = v - 0x2000;
                    break;
                default:
                    value = v - 0x10000000;
                    break;
            }
        }

        public static uint ReadDf2UInt(this BinaryReader reader) => reader.CoreReadDf2UInt(reader.ReadByte());

        private static uint CoreReadDf2UInt(this BinaryReader reader, byte first)
        {
            if ((first & 0x80) == 0)
                return first;

            if ((first & 0x40) == 0)
                return ((uint) (first & ~0x80) << 8)
                       | reader.ReadByte ();

            return ((uint) (first & ~0xc0) << 24)
                   | (uint) reader.ReadByte () << 16
                   | (uint) reader.ReadByte () << 8
                   | reader.ReadByte ();
        }

        public static int ReadDf2Int(this BinaryReader reader)
        {
            var b = reader.ReadByte();
            var u = (int) reader.CoreReadDf2UInt(b);
            var v = u >> 1;
            if ((u & 1) == 0)
                return v;

            switch (b & 0xc0)
            {
                case 0:
                case 0x40:
                    return v - 0x40;
                case 0x80:
                    return v - 0x2000;
                default:
                    return v - 0x10000000;
            }
        }

        public static void WriteDf2UInt(this BinaryWriter writer, uint value)
        {
            #if NETSTANDARD2_0
            var bytes = new byte[sizeof(uint)];
            var span = WriteBytes(bytes, value);
            writer.Write(bytes, 0, span.Length);
            #else
            Span<byte> bytes = stackalloc byte[sizeof(uint)];
            writer.Write(bytes);
#endif            
        }

        public static void WriteDf2Int(this BinaryWriter writer, int value)
        {
#if NETSTANDARD2_0
            var bytes = new byte[sizeof(int)];
            var span = WriteBytes(bytes, value);
            writer.Write(bytes, 0, span.Length);
#else
            Span<byte> bytes = stackalloc byte[sizeof(int)];
            writer.Write(bytes);
#endif            
        }

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
            writer.Write(value);
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
            if (left == right)
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
        public static T[] AsArray<T>(this IValue value) where T:unmanaged
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
    }
}