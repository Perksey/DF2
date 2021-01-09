using System;
using Ultz.DF2;

namespace Playground
{
    class Program
    {
        static void Main(string[] args)
        {
            TryRel("/RootData/Tables/Table0/DataStorage/", "/RootData/Tables/Table0/DataStorage/LongData");
            TryRel("/RootData/Tables/", "/RootData/Tables/Table0/DataStorage/LongData");
            TryRel("/RootData/Tables/Table0/DataStorage/", "/RootData/Tables/Table0/DataLength");
            TryRel("/RootData/Tables/Table0/DataStorage/", "/RootData/NumTables");
            TryRel("/RootData/Tables/Table0/DataStorage/", "/SomeOtherData/IntData");
            Console.WriteLine("-=-=-=-=-=-");
            Console.WriteLine();
            TryFull("/RootData/Tables/Table0/DataStorage/", "LongData");
            TryFull("/RootData/Tables/", "Table0/DataStorage/LongData");
            TryFull("/RootData/Tables/Table0/DataStorage/", "../DataLength");
            TryFull("/RootData/Tables/Table0/DataStorage/", "../../../NumTables");
            TryFull("/RootData/Tables/Table0/DataStorage/", "../../../../SomeOtherData/IntData");

            static void TryRel(string current, string dest)
            {
                Console.WriteLine($"Current Path: {current}");
                Console.WriteLine($"Destination Path: {dest}");
                Console.WriteLine($"Relative Path: {Df2Stream.GetRelativePath(dest, current)}");
                Console.WriteLine();
            }

            static void TryFull(string current, string dest)
            {
                Console.WriteLine($"Current Path: {current}");
                Console.WriteLine($"Destination Path: {dest}");
                Console.WriteLine($"Full Path: {Df2Stream.GetFullPath(dest, current)}");
                Console.WriteLine();
            }
        }
    }
}