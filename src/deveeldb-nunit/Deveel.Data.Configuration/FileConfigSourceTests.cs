using System;
using System.IO;
using System.Linq;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public sealed class FileConfigSourceTests {
		private const string FileName = "db.config";

		[SetUp]
		public void SetUp() {
			if (TestContext.CurrentContext.Test.Name == "LoadProperties") {
				CreatePropertiesFileToLoad();
			}
		}

		private void CreatePropertiesFileToLoad() {
			var path = Path.Combine(Environment.CurrentDirectory, FileName);
			using (var fileStream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024)) {
				using (var writer = new StreamWriter(fileStream)) {
					writer.WriteLine("# comment to properties");
					writer.WriteLine("first-setting: one");
					writer.WriteLine("second: 345.33");
				}
			}
		}

		[TearDown]
		public void TearDown() {
			if (TestContext.CurrentContext.Test.Name == "LoadProperties") {
				DeletePropertiesFile();
			}
		}

		private void DeletePropertiesFile() {
			var path = Path.Combine(Environment.CurrentDirectory, FileName);
			if (File.Exists(path))
				File.Delete(path);
		}

		[Test]
		public void LoadProperties() {
			var path = Path.Combine(Environment.CurrentDirectory, FileName);
			var source = new FileConfigSource(path);

			var dbConfig = new Configuration();
			dbConfig.Load(source, new PropertiesConfigFormatter());

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
