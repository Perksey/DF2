using System;
using System.Collections;

namespace Ultz.DF2
{
    public class CommandSender
    {
        private readonly Df2Stream _stream;

        public CommandSender(Df2Stream stream)
        {
            _stream = stream;
        }
        
        public void SendEnd()
        {
            _stream.BaseWriter!.Write((byte) Command.End);
        }
        public void SendGroup(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentNullException(nameof(path));
            }
            
            _stream.BaseWriter!.Write((byte) Command.Group);
            _stream.BaseWriter!.WriteDf2String(path);
        }
        public void SendValue(string name, object value, bool commandName = true, bool kind = true)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (value is not byte &&
                value is not sbyte &&
                value is not short &&
                value is not ushort &&
                value is not int &&
                value is not uint &&
                value is not long &&
                value is not ulong &&
                value is not float &&
                value is not double &&
                value is not string &&
                value is not byte[] &&
                value is not sbyte[] &&
                value is not short[] &&
                value is not ushort[] &&
                value is not int[] &&
                value is not uint[] &&
                value is not long[] &&
                value is not ulong[] &&
                value is not float[] &&
                value is not double[] &&
                value is not string[] &&
                value is not IEnumerable)
            {
                throw new ArgumentException(
                    "Value is of an invalid type. Valid types are (or inherit from) byte, sbyte, short, ushort, int, " +
                    "uint, long, ulong, float, double, string, an array of one of the aforementioned types, or " +
                    "IEnumerable");
            }

            if (commandName)
            {
                _stream.BaseWriter!.Write((byte) Command.Value);
            }

            if (value is byte byteValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Byte);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(byteValue);
            }
            else if (value is sbyte sbyteValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.SByte);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(sbyteValue);
            }
            else if (value is short shortValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Short);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2Int(shortValue);
            }
            else if (value is ushort ushortValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.UShort);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2UInt(ushortValue);
            }
            else if (value is int intValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Int);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2Int(intValue);
            }
            else if (value is uint uintValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.UInt);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2UInt(uintValue);
            }
            else if (value is long longValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Long);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                unsafe
                {
                    var ptr = (uint*) &longValue;
                    _stream.BaseWriter!.WriteDf2UInt(ptr[0]);
                    _stream.BaseWriter!.WriteDf2UInt(ptr[1]);
                }
            }
            else if (value is ulong ulongValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.ULong);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                unsafe
                {
                    var ptr = (uint*) &ulongValue;
                    _stream.BaseWriter!.WriteDf2UInt(ptr[0]);
                    _stream.BaseWriter!.WriteDf2UInt(ptr[1]);
                }
            }
            else if (value is float floatValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Float);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                unsafe
                {
                    _stream.BaseWriter!.WriteDf2UInt(*(uint*) &floatValue);
                }
            }
            else if (value is double doubleValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Double);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                unsafe
                {
                    var ptr = (uint*) &doubleValue;
                    _stream.BaseWriter!.WriteDf2UInt(ptr[0]);
                    _stream.BaseWriter!.WriteDf2UInt(ptr[1]);
                }
            }
            else if (value is string stringValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.String);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2String(stringValue);
            }
            else if (value is Array arrayValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.Array);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                var arrayKind = arrayValue switch
                {
                    _ when arrayValue is byte[] => ValueKind.Byte,
                    _ when arrayValue is sbyte[] => ValueKind.SByte,
                    _ when arrayValue is short[] => ValueKind.Short,
                    _ when arrayValue is ushort[] => ValueKind.UShort,
                    _ when arrayValue is int[] => ValueKind.Int,
                    _ when arrayValue is uint[] => ValueKind.UInt,
                    _ when arrayValue is long[] => ValueKind.Long,
                    _ when arrayValue is ulong[] => ValueKind.ULong,
                    _ when arrayValue is float[] => ValueKind.Float,
                    _ when arrayValue is double[] => ValueKind.Double,
                    _ when arrayValue is string[] => ValueKind.String,
                    _ => throw new InvalidOperationException("Invalid array type")
                };
                
                _stream.BaseWriter!.Write((byte)arrayKind);
                _stream.BaseWriter!.WriteDf2UInt((uint) arrayValue.Length);
                foreach (var obj in arrayValue)
                {
                    SendValue("<DO NOT SEND>", obj, false, false);
                }
            }
            else if (value is IEnumerable enumerableValue)
            {
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)ValueKind.List);
                }
                
                if (commandName)
                {
                    _stream.BaseWriter!.WriteDf2String(name);
                }
                
                foreach (var o in enumerableValue)
                {
                    SendValue("<DO NOT SEND>", o, false);
                }
                _stream.BaseWriter!.Write((byte)ValueKind.ListTerminator);
            }
        }
        public void SendRemove()
        {
            _stream.BaseWriter!.Write((byte) Command.Remove);
        }
        public void SendHandle()
        {
            _stream.BaseWriter!.Write((byte) Command.Handle);
        }
        public void SendEditValueByHandle()
        {
            _stream.BaseWriter!.Write((byte) Command.EditValueByHandle);
        }
        public void SendGroupByHandle()
        {
            _stream.BaseWriter!.Write((byte) Command.GroupByHandle);
        }
    }
}