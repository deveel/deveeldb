// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Globalization;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Configuration {
	public sealed class PropertiesConfigurationFormatter : IConfigurationFormatter {
		private void SetValue(IConfiguration config, string propKey, string value) {
			config.SetValue(propKey, value);
		}

		public void LoadInto(IConfiguration config, Stream inputStream) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");

			try {
				var properties = new Properties();
				properties.Load(inputStream);

				foreach (var entry in properties) {
					var propKey = entry.Key;
					SetValue(config, propKey, entry.Value);
				}
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not load properties from the given stream.", ex);
			}
		}

		public void SaveFrom(IConfiguration config, ConfigurationLevel level, Stream outputStream) {
			try {
				var keys = config.GetKeys(level);
				var properties = new Properties();

				foreach (var configKey in keys) {
					var configValue = config.GetValue(configKey);
					if (configValue != null) {
						var stringValue = Convert.ToString(configValue, CultureInfo.InvariantCulture);
						properties.SetProperty(configKey, stringValue);
					}
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