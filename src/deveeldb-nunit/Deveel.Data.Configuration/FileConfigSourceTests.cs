// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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
			var dbConfig = new Configuration();

			var path = Path.Combine(Environment.CurrentDirectory, FileName);
			using (var source = new FileConfigSource(path)) {
				dbConfig.Load(source, new PropertiesConfigFormatter());
			}

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
