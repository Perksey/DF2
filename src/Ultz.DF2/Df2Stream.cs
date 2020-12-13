using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Ultz.DF2
{
    /// <summary>
    /// Represents a bidirectional stream capable of transmitting and receiving DF2 data.
    /// </summary>
    public class Df2Stream : IDisposable
    {
        public Df2Stream(Stream @base)
        {
            if (@base.CanRead)
            {
                BaseReader = new BinaryReader(@base, Encoding.UTF8, true);
            }

            if (@base.CanWrite)
            {
                BaseWriter = new BinaryWriter(@base, Encoding.UTF8, true);
            }
        }
        
        public BinaryReader? BaseReader { get; }
        public BinaryWriter? BaseWriter { get; }
        public bool HasReceivedEnd { get; internal set; }
        public IValueDictionary Values { get; private set; }
        public Group? InboundCurrentGroup { get; internal set; }
        public Group? OutboundCurrentGroup { get; private set; }
        public Dictionary<uint, IValue> Handles { get; private set; }

        public void Dispose()
        {
            BaseReader?.Dispose();
        }

        public bool ProcessCommand()
        {
            if (BaseReader is null)
            {
                throw new NotSupportedException("Base stream was read-only at time of creation.");
            }
            
            if (HasReceivedEnd)
            {
                throw new InvalidOperationException("Previously received end command, will not read any further.");
            }
            

            return true;
        }

        public IValue? GetValue(string absolutePath)
        {
            absolutePath = absolutePath.TrimEnd('/');
            IValue? ret = null;
            foreach (var element in absolutePath.Split(new []{'/'}, StringSplitOptions.RemoveEmptyEntries))
            {
                if (ret is null)
                {
                    ret = Values[element];
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

        internal static string ToPath(string path) => path.Replace(Path.PathSeparator, '/')
            .Replace(Path.DirectorySeparatorChar, '/')
            .Replace(Path.VolumeSeparatorChar, '/')
            .Replace(Path.AltDirectorySeparatorChar, '/');
        internal static string GetFullPath(string path, string relativeTo) => ToPath(Path.GetFullPath(path, relativeTo));

        public void ProcessUntilEnd()
        {
            while (ProcessCommand())
            {
                // do nothing, this will loop until we get an End command
            }
        }

        public void Close()
        {
            var stream = BaseReader?.BaseStream ?? BaseWriter?.BaseStream;
            BaseReader?.Close();
            BaseWriter?.Close();
            stream?.Close();
        }
    }
}