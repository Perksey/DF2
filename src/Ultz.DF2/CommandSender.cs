using System;
using System.Collections;
using System.Data;

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

        public void SendValue(string name, object value, out ValueKind valueKind)
            => CoreSendValue(name, value, out valueKind);

        private void CoreSendValue(string name,
            object value,
            out ValueKind valueKind,
            bool commandName = true,
            bool kind = true,
            ValueKind? forceValue = null,
            uint? handle = null)
        {
            valueKind = default;
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
            
            if (value is byte byteValue)
            {
                Assert(forceValue, ValueKind.Byte);
                WritePreface(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Byte));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(byteValue);
            }
            else if (value is sbyte sbyteValue)
            {
                Assert(forceValue, ValueKind.SByte);
                WritePreface(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.SByte));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(sbyteValue);
            }
            else if (value is short shortValue)
            {
                Assert(forceValue, ValueKind.Short);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Short));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2Int(shortValue);
            }
            else if (value is ushort ushortValue)
            {
                Assert(forceValue, ValueKind.UShort);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.UShort));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2UInt(ushortValue);
            }
            else if (value is int intValue)
            {
                Assert(forceValue, ValueKind.Int);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Int));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2Int(intValue);
            }
            else if (value is uint uintValue)
            {
                Assert(forceValue, ValueKind.UInt);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.UInt));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2UInt(uintValue);
            }
            else if (value is long longValue)
            {
                Assert(forceValue, ValueKind.Long);
                WritePreface(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Long));
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
                Assert(forceValue, ValueKind.ULong);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.ULong));
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
                Assert(forceValue, ValueKind.Float);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Float));
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
                Assert(forceValue, ValueKind.Double);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Double));
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
                Assert(forceValue, ValueKind.String);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.String));
                }
                
                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2String(stringValue);
            }
            else if (value is Array arrayValue)
            {
                Assert(forceValue, ValueKind.Array);
                WritePreface(_stream, handle, commandName);
                
                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Array));
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
                    CoreSendValue("<DO NOT SEND>", obj, out _, false, false);
                }
            }
            else if (value is IEnumerable enumerableValue)
            {
                Assert(forceValue, ValueKind.List);
                WritePreface(_stream, handle, commandName);

                if (commandName)
                {
                    _stream.BaseWriter!.WriteDf2String(name);
                }

                foreach (var o in enumerableValue)
                {
                    CoreSendValue("<DO NOT SEND>", o, out _, false, false);
                }

                _stream.BaseWriter!.Write((byte) ValueKind.ListTerminator);
            }

            static void WritePreface(Df2Stream stream, uint? handle = null, bool commandName = true)
            {
                if (handle is not null)
                {
                    stream.BaseWriter!.Write((byte) Command.EditValueByHandle);
                    stream.BaseWriter!.WriteDf2UInt(handle.Value);
                }

                if (commandName)
                {
                    stream.BaseWriter!.Write((byte) Command.Value);
                }
            }

            static void Assert(ValueKind? forceValue, ValueKind type)
            {
                if (forceValue is not null && forceValue.Value != type)
                {
                    throw new DataException("Value is not of the forced type.");
                }
            }
        }
        public void SendRemove(string name)
        {
            _stream.BaseWriter!.Write((byte) Command.Remove);
            _stream.BaseWriter!.WriteDf2String(name);
        }
        public void SendHandle(string path, uint handle)
        {
            _stream.BaseWriter!.Write((byte) Command.Handle);
            _stream.BaseWriter!.WriteDf2String(path);
            _stream.BaseWriter!.WriteDf2UInt(handle);
        }
        public void SendEditValueByHandle(uint handle,
            ValueKind currentKind,
            object value,
            out ValueKind kind,
            string? fallbackPath)
        {
            try
            {
                CoreSendValue("<DO NOT SEND>", value, out _, false, false, currentKind, handle);
                kind = currentKind;
            }
            catch (DataException) // only thrown if we force a type which the value is not
            {
                if (fallbackPath is null)
                {
                    throw;
                }
                
                // fallback to send value
                SendValue(fallbackPath, value, out kind);
            }
        }
        public void SendGroupByHandle(uint handle)
        {
            _stream.BaseWriter!.Write((byte) Command.GroupByHandle);
            _stream.BaseWriter!.WriteDf2UInt(handle);
        }
    }
}