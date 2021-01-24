using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Ultz.DF2.Tests
{
    public class HandleWriteAndRecoverTests
    {
        public readonly ITestOutputHelper Out;

        public HandleWriteAndRecoverTests(ITestOutputHelper output)
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
        public void CanWriteAndRecoverByteOneElsewhereNested(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsByte());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteAndRecoverSByteOneElsewhereNested(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsSByte());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverShortOneElsewhereNested(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsShort());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteAndRecoverUShortOneElsewhereNested(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUShort());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverIntOneElsewhereNested(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsInt());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteAndRecoverUIntOneElsewhereNested(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUInt());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverLongOneElsewhereNested(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsLong());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteAndRecoverULongOneElsewhereNested(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsULong());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteAndRecoverFloatOneElsewhereNested(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsFloat());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteAndRecoverDoubleOneElsewhereNested(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsDouble());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteAndRecoverStringOneElsewhereNested(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.AddOrUpdate("TestData", testData);
            group.AddOrUpdate("TestData", testData);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsString());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoByteArraysOneNestedElsewhere(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<byte>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteAndRecoverTwoSByteArraysOneNestedElsewhere(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoShortArraysOneNestedElsewhere(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<short>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteAndRecoverTwoUShortArraysOneNestedElsewhere(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoIntArraysOneNestedElsewhere(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<int>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteAndRecoverTwoUIntArraysOneNestedElsewhere(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<uint>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoLongArraysOneNestedElsewhere(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<long>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteAndRecoverTwoULongArraysOneNestedElsewhere(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteAndRecoverTwoFloatArraysOneNestedElsewhere(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<float>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteAndRecoverTwoDoubleArraysOneNestedElsewhere(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<double>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteAndRecoverTwoStringArraysOneNestedElsewhere(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("GroupByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            group2.AddOrUpdate("TestData", testData);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<string>());
            Assert.Equal(testData, outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<string>());
        }

        [Theory, InlineData(byte.MinValue), InlineData(byte.MaxValue), InlineData(42)]
        public void CanWriteEditAndRecoverByteOneElsewhereNested(byte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (byte) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsByte());
            Assert.Equal(unchecked((byte) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsByte());
        }

        [Theory, InlineData(sbyte.MinValue), InlineData(sbyte.MaxValue), InlineData(42)]
        public void CanWriteEditAndRecoverSByteOneElsewhereNested(sbyte testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (sbyte) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsSByte());
            Assert.Equal(unchecked((sbyte) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsSByte());
        }

        [Theory, InlineData(short.MinValue), InlineData(short.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteEditAndRecoverShortOneElsewhereNested(short testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (short) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsShort());
            Assert.Equal(unchecked((short) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsShort());
        }

        [Theory, InlineData(ushort.MinValue), InlineData(ushort.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF)]
        public void CanWriteEditAndRecoverUShortOneElsewhereNested(ushort testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (ushort) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUShort());
            Assert.Equal(unchecked((ushort) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUShort());
        }

        [Theory, InlineData(int.MinValue), InlineData(int.MaxValue), InlineData(42), InlineData(0xFE), InlineData(0xFF),
         InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteEditAndRecoverIntOneElsewhereNested(int testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (int) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsInt());
            Assert.Equal(unchecked((int) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsInt());
        }

        [Theory, InlineData(uint.MinValue), InlineData(uint.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF)]
        public void CanWriteEditAndRecoverUIntOneElsewhereNested(uint testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (uint) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsUInt());
            Assert.Equal(unchecked((uint) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsUInt());
        }

        [Theory, InlineData(long.MinValue), InlineData(long.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteEditAndRecoverLongOneElsewhereNested(long testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (long) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsLong());
            Assert.Equal(unchecked((long) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsLong());
        }

        [Theory, InlineData(ulong.MinValue), InlineData(ulong.MaxValue), InlineData(42), InlineData(0xFE),
         InlineData(0xFF), InlineData(0xFFFE), InlineData(0xFFFF), InlineData(0xFFFFFE), InlineData(0xFFFFFF),
         InlineData(0xFFFFFFFE), InlineData(0xFFFFFFFF)]
        public void CanWriteEditAndRecoverULongOneElsewhereNested(ulong testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (ulong) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsULong());
            Assert.Equal(unchecked((ulong) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsULong());
        }

        [Theory, InlineData(float.MinValue), InlineData(float.MaxValue), InlineData(float.Epsilon),
         InlineData(float.PositiveInfinity), InlineData(float.NegativeInfinity), InlineData(float.NaN), InlineData(42)]
        public void CanWriteEditAndRecoverFloatOneElsewhereNested(float testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (float) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsFloat());
            Assert.Equal(unchecked((float) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsFloat());
        }

        [Theory, InlineData(double.MinValue), InlineData(double.MaxValue), InlineData(double.Epsilon),
         InlineData(double.PositiveInfinity), InlineData(double.NegativeInfinity), InlineData(double.NaN),
         InlineData(42)]
        public void CanWriteEditAndRecoverDoubleOneElsewhereNested(double testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += cmd =>
            {
                Out.WriteLine($"WRITE> {cmd}");
                if (cmd.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = (double) (testData + 1);
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsDouble());
            Assert.Equal(unchecked((double) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsDouble());
        }

        [Theory, InlineData("Forty Two"), InlineData("English 日本語 русский язык 官話"), InlineData("🌄"),
         MemberData(nameof(GetBigStrings))]
        public void CanWriteEditAndRecoverStringOneElsewhereNested(string testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            var group = inStream.GetOrAddGroup("TestGroup");
            group = group.GetOrAddGroup("TestNestedGroup");
            group.Handle = 1;
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            group.AddOrUpdate("TestData", testData);
            value2.Data = testData + 1;
            Assert.True(gotHandleCommand);
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsString());
            Assert.Equal(unchecked((string) (testData + 1)),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsString());
        }

        [Theory, InlineData(new byte[] {byte.MinValue, byte.MaxValue, 42})]
        public void CanWriteEditAndRecoverTwoByteArraysOneNestedElsewhere(byte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<byte>().Concat(new byte[] {unchecked(default(byte) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<byte>());
            Assert.Equal(testData.Concat(new[] {(byte) (unchecked(default(byte) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<byte>());
        }

        [Theory, InlineData(new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 42})]
        public void CanWriteEditAndRecoverTwoSByteArraysOneNestedElsewhere(sbyte[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<sbyte>().Concat(new sbyte[] {unchecked(default(sbyte) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
            Assert.Equal(testData.Concat(new[] {(sbyte) (unchecked(default(sbyte) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<sbyte>());
        }

        [Theory, InlineData(new short[] {short.MinValue, short.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteEditAndRecoverTwoShortArraysOneNestedElsewhere(short[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<short>().Concat(new short[] {unchecked(default(short) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<short>());
            Assert.Equal(testData.Concat(new[] {(short) (unchecked(default(short) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<short>());
        }

        [Theory, InlineData(new ushort[] {ushort.MinValue, ushort.MaxValue, 42, 0xFE, 0xFF})]
        public void CanWriteEditAndRecoverTwoUShortArraysOneNestedElsewhere(ushort[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<ushort>().Concat(new ushort[] {unchecked(default(ushort) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
            Assert.Equal(testData.Concat(new[] {(ushort) (unchecked(default(ushort) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ushort>());
        }

        [Theory, InlineData(new[] {int.MinValue, int.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteEditAndRecoverTwoIntArraysOneNestedElsewhere(int[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<int>().Concat(new int[] {unchecked(default(int) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<int>());
            Assert.Equal(testData.Concat(new[] {(int) (unchecked(default(int) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<int>());
        }

        [Theory, InlineData(new uint[] {uint.MinValue, uint.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF})]
        public void CanWriteEditAndRecoverTwoUIntArraysOneNestedElsewhere(uint[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<uint>().Concat(new uint[] {unchecked(default(uint) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<uint>());
            Assert.Equal(testData.Concat(new[] {(uint) (unchecked(default(uint) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<uint>());
        }

        [Theory,
         InlineData(new[]
         {
             long.MinValue, long.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteEditAndRecoverTwoLongArraysOneNestedElsewhere(long[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<long>().Concat(new long[] {unchecked(default(long) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<long>());
            Assert.Equal(testData.Concat(new[] {(long) (unchecked(default(long) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<long>());
        }

        [Theory,
         InlineData(new ulong[]
         {
             ulong.MinValue, ulong.MaxValue, 42, 0xFE, 0xFF, 0xFFFE, 0xFFFF, 0xFFFFFE, 0xFFFFFF, 0xFFFFFFFE, 0xFFFFFFFF
         })]
        public void CanWriteEditAndRecoverTwoULongArraysOneNestedElsewhere(ulong[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<ulong>().Concat(new ulong[] {unchecked(default(ulong) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
            Assert.Equal(testData.Concat(new[] {(ulong) (unchecked(default(ulong) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<ulong>());
        }

        [Theory,
         InlineData(new[]
         {
             float.MinValue, float.MaxValue, float.Epsilon, float.PositiveInfinity, float.NegativeInfinity, float.NaN,
             42
         })]
        public void CanWriteEditAndRecoverTwoFloatArraysOneNestedElsewhere(float[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<float>().Concat(new float[] {unchecked(default(float) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<float>());
            Assert.Equal(testData.Concat(new[] {(float) (unchecked(default(float) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<float>());
        }

        [Theory,
         InlineData(new[]
         {
             double.MinValue, double.MaxValue, double.Epsilon, double.PositiveInfinity, double.NegativeInfinity,
             double.NaN, 42
         })]
        public void CanWriteEditAndRecoverTwoDoubleArraysOneNestedElsewhere(double[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<double>().Concat(new double[] {unchecked(default(double) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<double>());
            Assert.Equal(testData.Concat(new[] {(double) (unchecked(default(double) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<double>());
        }

        [Theory, InlineData(new object[] {new[] {"Forty Two", "English 日本語 русский язык 官話", "🌄"}})]
        public void CanWriteEditAndRecoverTwoStringArraysOneNestedElsewhere(string[] testData)
        {
            using var s = new MemoryStream();
            using var inStream = new Df2Stream(s, StreamMode.Write, true);
            var group = inStream.GetOrAddGroup("TestGroup");
            var gotHandleCommand = false;
            inStream.CommandSend += msg =>
            {
                Out.WriteLine("WRITE> " + msg);
                if (msg.StartsWith("EditValueByHandle"))
                {
                    gotHandleCommand = true;
                }
            };
            group = group.GetOrAddGroup("TestNestedGroup");
            var group2 = inStream.GetOrAddGroup("TestGroup2");
            group2 = group2.GetOrAddGroup("TestNestedGroup");
            group2.Handle = 1;
            group.AddOrUpdate("TestData", testData);
            var value2 = group2.AddOrUpdate("TestData", testData);
            value2.Handle = 2;
            value2.Data = value2.AsArray<string>().Concat(new string[] {unchecked(default(string) + 1)}).ToArray();
            inStream.Flush();
            inStream.Close();
            Out.WriteLine($"Data: {Encoding.UTF8.GetString(s.ToArray())} ({BitConverter.ToString(s.ToArray())})");
            s.Seek(0, SeekOrigin.Begin);
            Assert.True(gotHandleCommand);
            using var outStream = new Df2Stream(s, StreamMode.Read);
            outStream.CommandReceive += msg => Out.WriteLine("READ> " + msg);
            outStream.ProcessUntilEnd();
            Assert.Equal(testData, outStream.Values["TestGroup"]["TestNestedGroup"]["TestData"].AsArray<string>());
            Assert.Equal(testData.Concat(new[] {(string) (unchecked(default(string) + 1))}),
                outStream["TestGroup2"]["TestNestedGroup"]["TestData"].AsArray<string>());
        }
    }
}