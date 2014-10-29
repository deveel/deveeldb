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
using System.Reflection;
using System.Text;

using NUnit.Framework;

namespace Deveel.Data.Configuration {
	[TestFixture]
	public class DbConfigTests {
		[Test]
		public void DefaultConfig() {
			IDbConfig config = null;
			Assert.DoesNotThrow(() => config = DbConfig.Default);
			Assert.IsNotNull(config);
			Assert.IsNull(config.Parent);
			Assert.IsNull(config.Source);
		}

		[Test]
		public void GetKeysFromRoot() {
			IDbConfig config = null;
			Assert.DoesNotThrow(() => config = DbConfig.Empty);
			Assert.IsNotNull(config);
			Assert.DoesNotThrow(() => config.SetKey(new ConfigKey("test.oneKey", 54, typeof(int))));
			Assert.DoesNotThrow(() => config.SetKey(new ConfigKey("test.twoKeys", typeof(string))));

			ConfigKey key1 = null;
			ConfigKey key2 = null;

			Assert.DoesNotThrow(() => key1 = config.GetKey("test.oneKey"));
			Assert.IsNotNull(key1);
			Assert.IsNotNull(key1.DefaultValue);
			Assert.AreEqual(typeof(int), key1.ValueType);
			Assert.AreEqual(54, key1.DefaultValue);

			Assert.DoesNotThrow(() => key2 = config.GetKey("test.twoKeys"));
			Assert.IsNotNull(key2);
			Assert.AreEqual(typeof(string), key2.ValueType);
			Assert.IsNull(key2.DefaultValue);
		}

		[Test]
		public void GetKeysFromChild() {
			IDbConfig config = null;
			Assert.DoesNotThrow(() => config = DbConfig.Default);
			Assert.IsNotNull(config);

			Assert.DoesNotThrow(() => config.SetKey(new ConfigKey("test.oneKey", "one", typeof(string))));

			IDbConfig child = null;
			Assert.DoesNotThrow(() => child = new DbConfig(config));
			Assert.IsNotNull(child);
			Assert.IsNotNull(child.Parent);

			Assert.DoesNotThrow(() => config.SetKey(new ConfigKey("test.oneKey", 45, typeof(int))));

			ConfigKey key = null;
			Assert.DoesNotThrow(() => key = config.GetKey("test.oneKey"));
			Assert.IsNotNull(key);
			Assert.AreEqual(typeof(int), key.ValueType);
			Assert.AreEqual(45, key.DefaultValue);
		}

		[Test]
		public void GetValueAsInt32() {
			IDbConfig config = null;
			Assert.DoesNotThrow(() => config = DbConfig.Default);
			Assert.IsNotNull(config);

			ConfigKey key = new ConfigKey("test.oneKey", "one", typeof(string));
			Assert.DoesNotThrow(() => config.SetKey(key));
			Assert.DoesNotThrow(() => config.SetValue(key, "22"));

			ConfigValue value = null;
			Assert.DoesNotThrow(() => value = config.GetValue(key));
			Assert.IsNotNull(value);
			Assert.IsNotNull(value.Value);

			int iValue = -1;
			Assert.DoesNotThrow(() => iValue = value.ToType<int>());
			Assert.AreEqual(22, iValue);
		}

		[Test]
		public void Extensions_LoadFromProperties() {
			var properties = new StringBuilder();
			properties.AppendLine("system.readOnly = false");
			properties.AppendLine("caching.type = Memory");

			IDbConfig dbConfig = null;
			Assert.DoesNotThrow(() => dbConfig = new DbConfig(new StringConfigSource(properties.ToString())));
			Assert.IsNotNull(dbConfig);
			Assert.DoesNotThrow(() => dbConfig.Load(new PropertiesConfigFormatter()));

			Assert.DoesNotThrow(() => Assert.IsNotNull(dbConfig.GetKey("system.readOnly")));

			bool readOnly = true;
			Assert.DoesNotThrow(() => readOnly = dbConfig.GetBoolean("system.readOnly"));
			Assert.IsFalse(readOnly);
		}
	}
}