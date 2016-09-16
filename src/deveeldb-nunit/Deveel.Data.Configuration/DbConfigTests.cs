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
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public class DbConfigTests {
		[Test]
		public void DefaultConfig() {
			IConfiguration config = null;
			Assert.DoesNotThrow(() => config = new Configuration());
			Assert.IsNotNull(config);
			Assert.IsNull(config.Parent);
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
			Assert.DoesNotThrow(() => configuration = new Configuration());
			Assert.IsNotNull(configuration);
			Assert.DoesNotThrow(() => configuration.Load(new StringConfigurationSource(properties.ToString()), new PropertiesConfigurationFormatter()));

			Assert.DoesNotThrow(() => Assert.IsNotNull(configuration.GetValue("system.readOnly")));

			bool readOnly = true;
			Assert.DoesNotThrow(() => readOnly = configuration.GetBoolean("system.readOnly"));
			Assert.IsFalse(readOnly);
		}
	}
}