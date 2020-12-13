using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Ultz.DF2
{
    public class Df2Reader : IDisposable
    {
        public Df2Reader(Stream input, bool leaveOpen = false)
        {
            BaseReader = new BinaryReader(input, Encoding.UTF8, leaveOpen);
        }
        
        public BinaryReader BaseReader { get; }
        public bool HasReceivedEnd { get; private set; }
        public Dictionary<string, Group> Groups { get; private set; }
        public Group? CurrentGroup { get; private set; }
        internal uint ResetToken { get; private set; }

        public void Dispose()
        {
            BaseReader?.Dispose();
        }

        public bool ProcessCommand()
        {
            if (HasReceivedEnd)
            {
                throw new InvalidOperationException("Previously received end command, will not read any further.");
            }
            
            var cmd = (Command) BaseReader.ReadByte();
            switch (cmd)
            {
                case Command.End:
                {
                    HasReceivedEnd = true;
                    return false;
                }
                case Command.Group:
                {
                    var path = BaseReader.ReadDf2String();
                    
                    break;
                }
                case Command.Value:
                    break;
                case Command.Remove:
                    break;
                case Command.Handle:
                    break;
                case Command.EditValueByHandle:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }
        
        

        public void ProcessUntilEnd()
        {
            while (ProcessCommand())
            {
                // do nothing, this will loop until we get an End command
            }
        }

        public void Reset()
        {
            HasReceivedEnd = false;
            ResetToken++;
        }
    }
}