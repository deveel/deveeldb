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
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public class DbConfigTests {
		[SetUp]
		public void TestSetup() {
			var testName = TestContext.CurrentContext.Test.Name;
			if (testName.StartsWith("LoadFromFile"))
				CreatePropertiesFile();
		}

		private void CreatePropertiesFile() {
			var propsFiile = Path.Combine(Environment.CurrentDirectory, "db.config");
			if (File.Exists(propsFiile))
				File.Delete(propsFiile);

			using (var stream = File.Create(propsFiile)) {
				using (var writer = new StreamWriter(stream)) {
					writer.WriteLine("system.configKey=7829");
					writer.WriteLine("db.name=testdb");
					writer.Flush();
				}
			}
		}

		[TearDown]
		public void TestTearDown() {
			var testName = TestContext.CurrentContext.Test.Name;
			if (testName.StartsWith("LoadFromFile") ||
				testName.StartsWith("SaveToFile")) {
				var propsFiile = Path.Combine(Environment.CurrentDirectory, "db.config");
				if (File.Exists(propsFiile))
					File.Delete(propsFiile);
			}
		}

		[Test]
		public void DefaultConfig() {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);
			Assert.IsNull(config.Parent);
			Assert.IsNull(config.Source);
		}

		[Test]
		public void GetValuesFromRoot() {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);
			Assert.DoesNotThrow(() => config.SetValue("test.oneKey", 54));
			Assert.DoesNotThrow(() => config.SetValue("test.twoKeys", null));

			object value1 = null;
			object value2 = null;

			Assert.DoesNotThrow(() => value1 = config.GetValue("test.oneKey"));
			Assert.IsNotNull(value1);
			Assert.IsInstanceOf<int>(value1);
			Assert.AreEqual(54, value1);

			Assert.DoesNotThrow(() => value2 = config.GetValue("test.twoKeys"));
			Assert.IsNull(value2);
		}

		[Test]
		public void GetValuesFromChild() {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);

			Assert.DoesNotThrow(() => config.SetValue("test.oneKey", "one"));

			IConfiguration child = null;
			Assert.DoesNotThrow(() => child = new Configuration(config));
			Assert.IsNotNull(child);
			Assert.IsNotNull(child.Parent);

			Assert.DoesNotThrow(() => child.SetValue("test.oneKey", 45));

			object value = null;
			Assert.DoesNotThrow(() => value = child.GetValue("test.oneKey"));
			Assert.IsNotNull(value);
			Assert.IsInstanceOf<int>(value);
			Assert.AreEqual(45, value);

			Assert.DoesNotThrow(() => value = config.GetValue("test.oneKey"));
			Assert.IsNotNull(value);
			Assert.IsInstanceOf<string>(value);
			Assert.AreEqual("one", value);
		}

		[Test]
		public void GetValueAsInt32() {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);

			Assert.DoesNotThrow(() => config.SetValue("test.oneKey", "22"));

			object value = null;
			Assert.DoesNotThrow(() => value = config.GetInt32("test.oneKey"));
			Assert.IsNotNull(value);
			Assert.IsInstanceOf<int>(value);
			Assert.AreEqual(22, value);
		}

		[TestCase("test","true", true)]
		[TestCase("test","false", false)]
		[TestCase("test", "off", false)]
		[TestCase("test", "on", true)]
		[TestCase("test", "enabled", true)]
		public void GetBooleanValue(string key, string value, bool expected) {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);

			Assert.DoesNotThrow(() => config.SetValue(key, value));

			object configValue = null;
			Assert.DoesNotThrow(() => configValue = config.GetBoolean(key));
			Assert.IsNotNull(configValue);
			Assert.IsInstanceOf<bool>(configValue);
			Assert.AreEqual(expected, configValue);
		}

		[TestCase(1, TestEnum.One)]
		[TestCase("one", TestEnum.One)]
		[TestCase("TWO", TestEnum.Two)]
		[TestCase(null, TestEnum.Default)]
		public void GetEnumValue(object value, TestEnum expected) {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);
			
			Assert.DoesNotThrow(() => config.SetValue("test", value));

			object configValue = null;
			Assert.DoesNotThrow(() => configValue = config.GetValue<TestEnum>("test"));
			Assert.IsInstanceOf<TestEnum>(configValue);
			Assert.AreEqual(expected, configValue);
		}

		public enum TestEnum {
			One = 1,
			Two = 2,
			Default = 0
		}

		[Test]
		public void Extensions_LoadFromProperties() {
			var properties = new StringBuilder();
			properties.AppendLine("system.readOnly = false");
			properties.AppendLine("caching.type = Memory");

			IConfiguration configuration = null;
			Assert.DoesNotThrow(() => configuration = new Configuration(new StringConfigSource(properties.ToString())));
			Assert.IsNotNull(configuration);
			Assert.DoesNotThrow(() => configuration.Load(new PropertiesConfigFormatter()));

			Assert.DoesNotThrow(() => Assert.IsNotNull(configuration.GetValue("system.readOnly")));

			bool readOnly = true;
			Assert.DoesNotThrow(() => readOnly = configuration.GetBoolean("system.readOnly"));
			Assert.IsFalse(readOnly);
		}

		[Test]
		public void LoadFromClosedStream() {
			var bytes = new byte[256];
			var stream = new MemoryStream(bytes, false);
			stream.Close();

			var config = new Configuration();

			Assert.Throws<DatabaseConfigurationException>(() => config.Load(stream));
		}

		[Test]
		public void LoadFromFile_Properties() {
			var filePath = Path.Combine(Environment.CurrentDirectory, "db.config");

			var config = new Configuration();
			using (var source = new FileConfigSource(filePath)) {
				config.Load(source, new PropertiesConfigFormatter());
			}

			Assert.AreEqual(2, config.Count());

			var keys = config.GetKeys().ToArray();
			Assert.IsNotEmpty(keys);
			Assert.AreEqual("system.configKey", keys.First());

			var configValue = config.GetValue("db.name");
			Assert.IsNotNull(configValue);
			Assert.IsInstanceOf<string>(configValue);
			Assert.AreEqual("testdb", (string)configValue);
		}

		[Test]
		public void SaveToFile_Properties() {
			var filePath = Path.Combine(Environment.CurrentDirectory, "db.config");

			var config = new Configuration();
			config.SetValue("system.configKey", 7679);
			config.SetValue("db.name", "testdb");

			config.Save(filePath, new PropertiesConfigFormatter());
		}
	}
}