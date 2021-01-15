using Xunit;

namespace Ultz.DF2.Tests
{
    public class PathTests
    {
        [Theory]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "/RootData/Tables/Table0/DataStorage/LongData", "LongData")]
        [InlineData("/RootData/Tables/", "/RootData/Tables/Table0/DataStorage/LongData", "Table0/DataStorage/LongData")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "/RootData/Tables/Table0/DataLength", "../DataLength")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "/RootData/NumTables", "../../../NumTables")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "/SomeOtherData/IntData",
            "../../../../SomeOtherData/IntData")]
        public void CanProduceRelativePaths(string current, string dest, string expected)
        {
            Assert.Equal(expected, Df2Stream.GetRelativePath(dest, current));
        }

        [Theory]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "LongData", "RootData/Tables/Table0/DataStorage/LongData")]
        [InlineData("/RootData/Tables/", "Table0/DataStorage/LongData", "RootData/Tables/Table0/DataStorage/LongData")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "../DataLength", "RootData/Tables/Table0/DataLength")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "../../../NumTables", "RootData/NumTables")]
        [InlineData("/RootData/Tables/Table0/DataStorage/", "../../../../SomeOtherData/IntData",
            "SomeOtherData/IntData")]
        public void CanProduceFullPaths(string current, string dest, string expected)
        {
            Assert.Equal(expected, Df2Stream.GetFullPath(dest, current));
        }
    }
}