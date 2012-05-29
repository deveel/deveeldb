using System;
using System.IO;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Control {
	[TestFixture]
	public sealed class DbConfigTest {
		[Test]
		public void LoadTest() {
			StringBuilder sb = new StringBuilder();
			StringWriter writer = new StringWriter(sb);

			writer.WriteLine("key1=value1");
			writer.WriteLine("key2 = on");
			writer.WriteLine("key3= true");
			writer.Flush();

			MemoryStream stream = new MemoryStream(Encoding.GetEncoding("ISO-8859-1").GetBytes(sb.ToString()));

			DbConfig config = new DbConfig();
			config.LoadFrom(stream);

			string value = config.GetValue<string>("key1");
			Assert.IsNotNull(value, "The 'key1' was not set into the config.");
			Assert.IsNotEmpty(value, "The value of 'key1' is empty.");
			Assert.AreEqual("value1", value, "The value for 'key1' is not correct.");

			value = config.GetValue<string>("key2");
			Assert.IsNotNull(value, "The 'key2' was not set into the config.");
			Assert.IsNotEmpty(value, "The value of 'key2' is empty.");
			Assert.AreEqual("on", value, "The value for 'key2' is not correct.");

			value = config.GetValue<string>("key3");
			Assert.IsNotNull(value, "The 'key3' was not set into the config.");
			Assert.IsNotEmpty(value, "The value of 'key3' is empty.");
			Assert.AreEqual("true", value, "The value for 'key1' is not correct.");
		}

		[Test]
		public void SaveToTest() {
			DbConfig config = new DbConfig();
			config.SetValue("key1", "value1");
			config.SetValue("key2", "on");
			config.SetValue("key3", "true");

			MemoryStream stream = new MemoryStream();
			config.SaveTo(stream, ConfigFormatterType.Properties);

		}
	}
}