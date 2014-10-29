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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Configuration {
	public sealed class PropertiesConfigFormatter : IConfigFormatter {
		private void SetValue(IDbConfig config, string propKey, string value) {
			var configKey = config.GetKey(propKey);
			if (configKey != null) {
				var propValue = ConvertValueTo(value, configKey.ValueType);
				config.SetValue(configKey, propValue);
			} else {
				configKey = new ConfigKey(propKey, typeof (string));
				config.SetKey(configKey);
				config.SetValue(configKey, value);
			}
		}

		private object ConvertValueTo(string value, Type valueType) {
			return Convert.ChangeType(value, valueType, CultureInfo.InvariantCulture);
		}

		public void LoadInto(IDbConfig config, Stream inputStream) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");

			try {
				var properties = new Properties();
				properties.Load(inputStream);

				foreach (DictionaryEntry entry in properties) {
					var propKey = (string) entry.Key;
					SetValue(config, propKey, (string) entry.Value);
				}
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not load properties from the given stream.", ex);
			}
		}

		public void SaveFrom(IDbConfig config, ConfigurationLevel level, Stream outputStream) {
			try {
				var keys = config.GetKeys(level);
				var properties = new Properties();

				foreach (var configKey in keys) {
					var configValue = config.GetValue(configKey);
					object value;
					if (configValue == null || configValue.Value == null) {
						value = configKey.DefaultValue;
					} else {
						value = configValue.Value;
					}

					var stringValue = Convert.ToString(value, CultureInfo.InvariantCulture);
					properties.SetProperty(configKey.Name, stringValue);
				}

				properties.Store(outputStream, String.Empty);
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not save the configurations to the given stream.", ex);
			}
		}
	}
}