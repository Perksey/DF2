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
            var cmd = (Command) _stream.BaseReader!.ReadByte();
            switch (cmd)
            {
                case Command.End:
                {
                    _stream.HasReceivedEnd = true;
                    return false;
                }
                case Command.Group:
                {
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        if (path == string.Empty)
                        {
                            _stream.InboundCurrentGroup = null;
                        }
                        

                        throw new InvalidOperationException();
                    }

                    var previousPath = new Uri(new Uri(path), ".").ToString();
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
                        parentGroupDictionary.Add(value = new Group(parentValue ?? (object) _stream, name));
                    }

                    _stream.InboundCurrentGroup = (Group) value;
                    break;
                }
                case Command.Value:
                {
                    var kind = (ValueKind) _stream.BaseReader.ReadByte();
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        throw new InvalidOperationException("Path should not be null or whitespace");
                    }

                    var previousPath = new Uri(new Uri(path), ".").ToString();
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
                        ((Value) val).UpdateValue(kind, ReadValue(kind, out _));
                    }
                    else
                    {
                        currentDictionary.Add(new Value((IGroupInternal)parentValue ?? _stream, name, kind,
                            ReadValue(kind, out _)));
                    }

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

                    var previousPath = new Uri(new Uri(path), ".").ToString();
                    var parentValue = _stream.GetValue(previousPath);
                    if (parentValue is Value)
                    {
                        throw new InvalidOperationException(
                            "The path to contain the group refers to a value instead of a group or the document root.");
                    }

                    var parentGroupDictionary =
                        (ValueDictionary) (parentValue is null ? _stream.Values : ((Group) parentValue).Values);
                    parentGroupDictionary.Remove(Path.GetFileName(path));
                    break;
                }
                case Command.Handle:
                {
                    var path = Df2Stream.GetFullPath(_stream.BaseReader.ReadDf2String(),
                        _stream.InboundCurrentGroup?.AbsolutePath ?? "/").TrimEnd('/');
                    if (string.IsNullOrWhiteSpace(path))
                    {
                        ((IDictionary<uint, IValue>) _stream.Handles).Remove(_stream.BaseReader.ReadDf2UInt());
                        break;
                    }

                    var value = _stream.GetValue(path);
                    if (value is null)
                    {
                        throw new InvalidOperationException("Can't assign a handle to the root of the document");
                    }

                    ((IValueInternal)value).UpdateHandle(_stream.BaseReader.ReadDf2UInt());
                    break;
                }
                case Command.EditValueByHandle:
                {
                    var val = _stream.Handles[_stream.BaseReader.ReadDf2UInt()];
                    if (val is not Value value)
                    {
                        throw new InvalidOperationException("Can only set a value of a handle referring to a value");
                    }
                    
                    value.UpdateValue(val.Kind, ReadValue(val.Kind, out _));
                    break;
                }
                case Command.GroupByHandle:
                {
                    var val = _stream.Handles[_stream.BaseReader.ReadDf2UInt()];
                    if (val is not Group value)
                    {
                        throw new InvalidOperationException("Can only set a value of a handle referring to a value");
                    }

                    _stream.InboundCurrentGroup = value;
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
                {
                    unsafe
                    {
                        var ret = stackalloc uint[2];
                        ret[0] = _stream.BaseReader!.ReadDf2UInt();
                        ret[1] = _stream.BaseReader!.ReadDf2UInt();
                        return *(long*) ret;
                    }
                }
                case ValueKind.Float:
                {
                    unsafe
                    {
                        var ret = stackalloc uint[1];
                        ret[0] = _stream.BaseReader!.ReadDf2UInt();
                        return *(float*) ret;
                    }
                }
                case ValueKind.Double:
                {
                    unsafe
                    {
                        var ret = stackalloc uint[2];
                        ret[0] = _stream.BaseReader!.ReadDf2UInt();
                        ret[1] = _stream.BaseReader!.ReadDf2UInt();
                        return *(double*) ret;
                    }
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
                {
                    unsafe
                    {
                        var val = _stream.BaseReader!.ReadByte();
                        return *(sbyte*) &val;
                    }
                }
                case ValueKind.UShort:
                    return (ushort) _stream.BaseReader!.ReadDf2UInt();
                case ValueKind.UInt:
                    return _stream.BaseReader!.ReadDf2UInt();
                case ValueKind.ULong:
                {
                    unsafe
                    {
                        var ret = stackalloc uint[2];
                        ret[0] = _stream.BaseReader!.ReadDf2UInt();
                        ret[1] = _stream.BaseReader!.ReadDf2UInt();
                        return *(ulong*) ret;
                    }
                }
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