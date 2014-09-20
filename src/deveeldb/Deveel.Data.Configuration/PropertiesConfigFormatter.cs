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

using Deveel.Data.Deveel.Data.Configuration;
using Deveel.Data.Util;

namespace Deveel.Data.Configuration {
	public sealed class PropertiesConfigFormatter : IConfigFormatter {
		public void LoadInto(IDbConfig config, Stream inputStream) {
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");

			try {
				var properties = new Properties();
				properties.Load(inputStream);

				foreach (DictionaryEntry entry in properties) {
					config.SetValue((string) entry.Key, entry.Value);
				}
			} catch (DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not load properties from the given stream.", ex);
			}
		}

		public void SaveFrom(IDbConfig config, Stream outputStream) {
			try {
				var properties = new Properties();

				foreach (KeyValuePair<string, object> pair in config) {
					var stringValue = Convert.ToString(pair.Value, CultureInfo.InvariantCulture);
					properties.SetProperty(pair.Key, stringValue);
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