using System;
using System.IO;
using System.Linq;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public static class StreamConfigSourceTests {
		private static Stream CreatePropertiesStreamToLoad() {
			var stream = new MemoryStream(1024);
			var writer = new StreamWriter(stream);
			writer.WriteLine("# comment to properties");
			writer.WriteLine("first-setting: one");
			writer.WriteLine("second: 345.33");
			writer.Flush();

			stream.Seek(0, SeekOrigin.Begin);
			return stream;
		}

		[Test]
		public static void LoadProperties() {
			var stream = CreatePropertiesStreamToLoad();

			var dbConfig = new Configuration();
			dbConfig.Load(stream, new PropertiesConfigFormatter());

			Assert.AreEqual(2, dbConfig.GetKeys().Count());

			var firstSetting = dbConfig.GetValue("first-setting");
			Assert.IsNotNull(firstSetting);
			Assert.IsInstanceOf<string>(firstSetting);
			Assert.AreEqual("one", firstSetting);

			var secondSetting = dbConfig.GetValue("second");
			Assert.IsNotNull(secondSetting);
			Assert.IsInstanceOf<string>(secondSetting);
			Assert.AreEqual("345.33", secondSetting);
		}
	}
}