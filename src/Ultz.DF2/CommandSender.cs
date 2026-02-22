using System;
using System.Collections;
using System.Data;
using System.Linq;

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
            if (_stream.BaseWriter is null)
            {
                return;
            }


            _stream.CoreSendEvent("End;");

            _stream.BaseWriter!.Write((byte) Command.End);
        }

        public void SendGroup(string path)
        {
            if (_stream.BaseWriter is null)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(path) && path != string.Empty)
            {
                throw new ArgumentNullException(nameof(path));
            }


            _stream.CoreSendEvent($"Group ({path});");

            _stream.BaseWriter!.Write((byte) Command.Group);
            _stream.BaseWriter!.WriteDf2String(path);
        }

        public void SendValue(string name, object value, out ValueKind valueKind)
        {
            if (_stream.BaseWriter is null)
            {
                valueKind = ValueKind.Null;
                return;
            }

            CoreSendValue(name, value, out valueKind);

            _stream.CoreSendEvent($"Value {valueKind} ({name}) ({Helpers.CoreToString(value)});");

        }

        private void CoreSendValue(string name,
            object? value,
            out ValueKind valueKind,
            bool commandName = true,
            bool kind = true,
            ValueKind? forceValue = null,
            uint? handle = null)
        {
            if (_stream.BaseWriter is null)
            {
                valueKind = ValueKind.Null;
                return;
            }

            valueKind = default;
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (value is byte byteValue)
            {
                Assert(forceValue, ValueKind.Byte);
                WriteValueCommand(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Byte));
                }

                if (commandName)
                {
                    _stream.BaseWriter!.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(byteValue);
            }
            else if (value is sbyte sbyteValue)
            {
                Assert(forceValue, ValueKind.SByte);
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Long));
                }

                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2Int64(longValue);
            }
            else if (value is ulong ulongValue)
            {
                Assert(forceValue, ValueKind.ULong);
                WriteValueCommand(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.ULong));
                }

                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.WriteDf2UInt64(ulongValue);
            }
            else if (value is float floatValue)
            {
                Assert(forceValue, ValueKind.Float);
                WriteValueCommand(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Float));
                }

                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(floatValue);
            }
            else if (value is double doubleValue)
            {
                Assert(forceValue, ValueKind.Double);
                WriteValueCommand(_stream, handle, commandName);

                if (kind)
                {
                    _stream.BaseWriter!.Write((byte)(valueKind = ValueKind.Double));
                }

                if (commandName)
                {
                    _stream.BaseWriter.WriteDf2String(name);
                }

                _stream.BaseWriter!.Write(doubleValue);
            }
            else if (value is string stringValue)
            {
                Assert(forceValue, ValueKind.String);
                WriteValueCommand(_stream, handle, commandName);

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
                WriteValueCommand(_stream, handle, commandName);

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
                    _ when arrayValue.GetType() == typeof(byte[]) => ValueKind.Byte,
                    _ when arrayValue.GetType() == typeof(sbyte[]) => ValueKind.SByte,
                    _ when arrayValue.GetType() == typeof(short[]) => ValueKind.Short,
                    _ when arrayValue.GetType() == typeof(ushort[]) => ValueKind.UShort,
                    _ when arrayValue.GetType() == typeof(int[]) => ValueKind.Int,
                    _ when arrayValue.GetType() == typeof(uint[]) => ValueKind.UInt,
                    _ when arrayValue.GetType() == typeof(long[]) => ValueKind.Long,
                    _ when arrayValue.GetType() == typeof(ulong[]) => ValueKind.ULong,
                    _ when arrayValue.GetType() == typeof(float[]) => ValueKind.Float,
                    _ when arrayValue.GetType() == typeof(double[]) => ValueKind.Double,
                    _ when arrayValue.GetType() == typeof(string[]) => ValueKind.String,
                    _ => throw new InvalidOperationException("Invalid array type")
                };

                _stream.BaseWriter!.Write((byte)arrayKind);
                _stream.BaseWriter!.WriteDf2UInt((uint)arrayValue.Length);
                foreach (var obj in arrayValue)
                {
                    CoreSendValue("<DO NOT SEND>", obj, out _, false, false);
                }
            }
            else if (value is IEnumerable enumerableValue)
            {
                Assert(forceValue, ValueKind.List);
                WriteValueCommand(_stream, handle, commandName);

                if (commandName)
                {
                    _stream.BaseWriter!.WriteDf2String(name);
                }

                foreach (var o in enumerableValue)
                {
                    CoreSendValue("<DO NOT SEND>", o, out _, false, false);
                }

                _stream.BaseWriter!.Write((byte)ValueKind.ListTerminator);
            }
            else if (value is null)
            {
                Assert(forceValue, ValueKind.Null);
                WriteValueCommand(_stream, handle, commandName);
            }
            else
            {
                throw new ArgumentException(
                    "Value is of an invalid type. Valid types are byte, sbyte, short, ushort, int, " +
                    "uint, long, ulong, float, double, string, an array of one of the aforementioned types, or a type which implements " +
                    "IEnumerable");
            }

            static void WriteValueCommand(Df2Stream stream, uint? handle = null, bool commandName = true)
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
            if (_stream.BaseWriter is null)
            {
                return;
            }

            _stream.BaseWriter!.Write((byte) Command.Remove);
            _stream.BaseWriter!.WriteDf2String(name);
#if DEBUG
            _stream.CoreSendEvent($"Remove ({name});");
#endif
        }

        public void SendHandle(string path, uint handle)
        {
            if (_stream.BaseWriter is null)
            {
                return;
            }

            _stream.BaseWriter!.Write((byte) Command.Handle);
            _stream.BaseWriter!.WriteDf2String(path);
            _stream.BaseWriter!.WriteDf2UInt(handle);
#if DEBUG
            _stream.CoreSendEvent($"Handle ({path}) {handle};");
#endif
        }

        public void SendEditValueByHandle(uint handle,
            ValueKind currentKind,
            object value,
            out ValueKind kind,
            string? fallbackPath)
        {
            if (_stream.BaseWriter is null)
            {
                kind = ValueKind.Null;
                return;
            }

            var usedHandle = false;
            try
            {
                CoreSendValue("<DO NOT SEND>", value, out _, false, false, currentKind, handle);
                kind = currentKind;
                usedHandle = true;
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

#if DEBUG
            _stream.CoreSendEvent(usedHandle
                ? $"EditValueByHandle {handle} ({value});"
                : $"Value {kind} ({fallbackPath}) ({value});");
#endif
        }

        public void SendGroupByHandle(uint handle)
        {
            if (_stream.BaseWriter is null)
            {
                return;
            }

#if DEBUG
            _stream.CoreSendEvent($"GroupByHandle {handle};");
#endif
            _stream.BaseWriter!.Write((byte) Command.GroupByHandle);
            _stream.BaseWriter!.WriteDf2UInt(handle);
        }
    }
}