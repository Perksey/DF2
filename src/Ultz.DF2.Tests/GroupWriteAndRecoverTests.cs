using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Ultz.DF2.Tests
{
    public class GroupWriteAndRecoverTests
    {
        public readonly ITestOutputHelper Out;

        public GroupWriteAndRecoverTests(ITestOutputHelper output)
        {
            Out = output;
        }

        public static IEnumerable<object[]> GetBigStrings()
        {
            yield return new object[] {new string('A', 1024 * 1024)};
            const string text = "English 日本語 русский язык 官話 🌄 ";
            var bytes = Encoding.UTF8.GetBytes(text);
            var numRepeats = (int) (1024 * 1024 / (double) bytes.Length);
            yield return new object[] {string.Join(string.Empty, Enumerable.Range(0, numRepeats).Select(_ => text))};
        }

        [Theory, InlineData(byte.MinValue), InlineData(byte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverByte(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverSByte(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverShort(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverUShort(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverInt(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverUInt(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverLong(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverULong(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteAndRecoverFloat(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteAndRecoverDouble(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += cmd => Out.WriteLine($"WRITE> {cmd}");
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteAndRecoverString(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            inStream.CommandSend += msg => Out.WriteLine("SEND> " + msg);
            var group = inStream.GetOrAddGroup("TestGroup");
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteAndRecoverByteArray(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteAndRecoverSByteArray(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverShortArray(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverUShortArray(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverIntArray(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverUIntArray(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverLongArray(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverULongArray(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteAndRecoverFloatArray(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteAndRecoverDoubleArray(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteAndRecoverStringArray(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            inStream.CommandSend += msg => Out.WriteLine("WRITE> " + msg);
            group.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestData"].AsArray<string>());
        }
    }
}