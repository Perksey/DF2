using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ultz.DF2
{
    internal class CommandReceiver
    {
        private readonly Df2Stream _stream;

        public CommandReceiver(Df2Stream stream)
        {
            _stream = stream;
        }

        public bool ProcessCommand()
        {
            if (_stream.BaseReader is null)
            {
                throw new NotSupportedException("Stream is not configured for reading");
            }

            var cmd = (Command) _stream.BaseReader!.ReadByte();
            switch (cmd)
            {
                case Command.End:
                {
                    _stream.HasReceivedEnd = true;
#if DEBUG
                    _stream.CoreReceiveEvent("End;");
#endif
                    return false;
                }
                case Command.Group:
                {
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/");
#if DEBUG
                    _stream.CoreReceiveEvent($"Group ({path});");
#endif
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        if (path == string.Empty)
                        {
                            _stream.InboundCurrentGroup = null;
                            break;
                        }

                        throw new InvalidOperationException();
                    }

                    var previousPath = Df2Stream.GetFullPath("..", path);
                    var parentValue = _stream.GetValue(previousPath);
                    if (parentValue is Value)
                    {
                        throw new InvalidOperationException(
                            "The path to contain the group refers to a value instead of a group or the document root.");
                    }

                    var parentGroupDictionary =
                        (ValueDictionary) (parentValue is null ? _stream.Values : ((Group) parentValue).Values);

                    var name = Path.GetFileName(path);
                    if (!parentGroupDictionary.TryGetValue(name, out var value))
                    {
                        parentGroupDictionary.Add(value = new Group(parentValue ?? _stream, name));
                    }

                    _stream.InboundCurrentGroup = (Group) value;
                    break;
                }
                case Command.Value:
                {
                    var kind = (ValueKind) _stream.BaseReader.ReadByte();
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');
                    ValueTuple<ValueKind, string, object> recv = (kind, path, null);
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        throw new InvalidOperationException("Path should not be null or whitespace");
                    }

                    var previousPath = Df2Stream.GetFullPath("..", path);
                    var parentValue = _stream.GetValue(previousPath);
                    if (parentValue is Value)
                    {
                        throw new InvalidOperationException(
                            "The path to contain the group refers to a value instead of a group or the document root.");
                    }

                    var currentDictionary =
                        (ValueDictionary) (parentValue is null ? _stream.Values : ((Group) parentValue).Values);
                    var name = Path.GetFileName(path);
                    if (currentDictionary.TryGetValue(name, out var val))
                    {
                        ((Value) val).UpdateValue(kind, recv.Item3 = ReadValue(kind, out _));
                    }
                    else
                    {
                        currentDictionary.Add(new Value((IGroupInternal) parentValue ?? _stream, name, kind,
                            recv.Item3 = ReadValue(kind, out _)));
                    }

#if DEBUG
                    _stream.CoreReceiveEvent(
                        $"Value {recv.Item1} ({recv.Item2}) ({Helpers.CoreToString(recv.Item3)});");
#endif

                    break;
                }
                case Command.Remove:
                {
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        throw new InvalidOperationException("Path should not be null or whitespace");
                    }

#if DEBUG
                    _stream.CoreReceiveEvent($"Remove ({path});");
#endif

                    var previousPath = Df2Stream.GetFullPath("..", path);
                    var parentValue = _stream.GetValue(previousPath);
                    var parentGroupDictionary =
                        (ValueDictionary) (parentValue is null ? _stream.Values : ((Group) parentValue).Values);
                    parentGroupDictionary.Remove(Path.GetFileName(path));
                    break;
                }
                case Command.Handle:
                {
                    var rawPath = _stream.BaseReader.ReadDf2String();
                    if (string.IsNullOrWhiteSpace(rawPath))
                    {
                        ((IDictionary<uint, IValue>) _stream.Handles).Remove(_stream.BaseReader.ReadDf2UInt());
                        break;
                    }

                    var path = Df2Stream.GetFullPath(rawPath,
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');

                    var value = _stream.GetValue(path);
                    if (value is null)
                    {
                        throw new InvalidOperationException("Can't assign a handle to the root of the document");
                    }

                    uint handle;
                    ((IValueInternal) value).UpdateHandle(handle = _stream.BaseReader.ReadDf2UInt());
                    ((IDictionary<uint, IValue>) _stream.Handles).Add(handle, value);
#if DEBUG
                    _stream.CoreReceiveEvent($"Handle ({path}) {handle};");
#endif
                    break;
                }
                case Command.EditValueByHandle:
                {
                    uint handle;
                    var val = _stream.Handles[handle = _stream.BaseReader.ReadDf2UInt()];
                    if (val is not Value value)
                    {
                        throw new InvalidOperationException("Can only set a value of a handle referring to a value");
                    }

                    object read;
                    value.UpdateValue(val.Kind, read = ReadValue(val.Kind, out _));
#if DEBUG
                    _stream.CoreReceiveEvent($"EditValueByHandle {handle} ({Helpers.CoreToString(read)});");
#endif
                    break;
                }
                case Command.GroupByHandle:
                {
                    uint handle;
                    var val = _stream.Handles[handle = _stream.BaseReader.ReadDf2UInt()];
                    if (val is not Group value)
                    {
                        throw new InvalidOperationException("Can only set a value of a handle referring to a value");
                    }

                    _stream.InboundCurrentGroup = value;
#if DEBUG
                    _stream.CoreReceiveEvent($"GroupByHandle {handle};");
#endif
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        private object ReadValue(ValueKind kind, out bool listTerminated)
        {
            listTerminated = false;
            switch (kind)
            {
                case ValueKind.Null:
                    return null;
                case ValueKind.Byte:
                    return _stream.BaseReader!.ReadByte();
                case ValueKind.Short:
                    return (short) _stream.BaseReader!.ReadDf2Int();
                case ValueKind.Int:
                    return _stream.BaseReader!.ReadDf2Int();
                case ValueKind.Long:
                    return _stream.BaseReader!.ReadDf2Int64();
                case ValueKind.Float:
                {
                    return _stream.BaseReader!.ReadSingle();
                }
                case ValueKind.Double:
                {
                    return _stream.BaseReader!.ReadDouble();
                }
                case ValueKind.String:
                {
                    return _stream.BaseReader.ReadDf2String();
                }
                case ValueKind.Group:
                {
                    throw new InvalidOperationException("Groups are not values");
                }
                case ValueKind.SByte:
                    return _stream.BaseReader!.ReadSByte();
                case ValueKind.UShort:
                    return (ushort) _stream.BaseReader!.ReadDf2UInt();
                case ValueKind.UInt:
                    return _stream.BaseReader!.ReadDf2UInt();
                case ValueKind.ULong:
                    return _stream.BaseReader!.ReadDf2UInt64();
                case ValueKind.Array:
                {
                    var arrayKind = (ValueKind) _stream.BaseReader!.ReadByte();
                    var len = _stream.BaseReader!.ReadDf2UInt();
                    return CoreReadArray(this, len, arrayKind);
                }
                case ValueKind.List:
                {
                    return CoreReadList(this).ToArray();
                }
                case ValueKind.ListTerminator:
                {
                    listTerminated = true;
                    return null;
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
            }

            static object CoreReadArray(CommandReceiver receiver, uint len, ValueKind kind)
            {
                switch (kind)
                {
                    case ValueKind.Byte: return CoreBulkRead(receiver, len, kind).Cast<byte>().ToArray();
                    case ValueKind.SByte: return CoreBulkRead(receiver, len, kind).Cast<sbyte>().ToArray();
                    case ValueKind.Short: return CoreBulkRead(receiver, len, kind).Cast<short>().ToArray();
                    case ValueKind.UShort: return CoreBulkRead(receiver, len, kind).Cast<ushort>().ToArray();
                    case ValueKind.Int: return CoreBulkRead(receiver, len, kind).Cast<int>().ToArray();
                    case ValueKind.UInt: return CoreBulkRead(receiver, len, kind).Cast<uint>().ToArray();
                    case ValueKind.Long: return CoreBulkRead(receiver, len, kind).Cast<long>().ToArray();
                    case ValueKind.ULong: return CoreBulkRead(receiver, len, kind).Cast<ulong>().ToArray();
                    case ValueKind.Float: return CoreBulkRead(receiver, len, kind).Cast<float>().ToArray();
                    case ValueKind.Double: return CoreBulkRead(receiver, len, kind).Cast<double>().ToArray();
                    case ValueKind.String: return CoreBulkRead(receiver, len, kind).Cast<string>().ToArray();
                    case ValueKind.Array:
                    case ValueKind.List:
                        throw new NotImplementedException(
                            "Arrays and lists within arrays currently aren't implemented.");
                    case ValueKind.Group:
                    case ValueKind.ListTerminator:
                        throw new NotSupportedException("Invalid array type");
                    default:
                        throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
                }
            }

            static IEnumerable<object> CoreBulkRead(CommandReceiver receiver, uint len, ValueKind kind)
            {
                for (var i = 0; i < len; i++)
                {
                    yield return receiver.ReadValue(kind, out _);
                }
            }

            static IEnumerable<object> CoreReadList(CommandReceiver receiver)
            {
                var terminated = false;
                while (!terminated)
                {
                    var elementKind = (ValueKind) receiver._stream.BaseReader!.ReadByte();
                    if (elementKind == ValueKind.Group)
                    {
                        throw new InvalidOperationException("Group cannot be contained within an array or list.");
                    }

                    var ret = receiver.ReadValue(elementKind, out terminated);
                    if (!terminated)
                    {
                        yield return ret;
                    }
                }
            }
        }
    }
}