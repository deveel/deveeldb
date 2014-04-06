using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Configuration {
	public sealed class PropertiesConfigFormatter : IConfigFormatter {
		public void LoadInto(IDbConfig config, Stream inputStream) {
			var properties = new Properties();
			properties.Load(inputStream);

			foreach (DictionaryEntry entry in properties) {
				config.SetValue((string) entry.Key, entry.Value);
			}
		}

		public void SaveFrom(IDbConfig config, Stream outputStream) {
			var properties = new Properties();

			foreach (KeyValuePair<string, object> pair in config) {
				var stringValue = Convert.ToString(pair.Value, CultureInfo.InvariantCulture);
				properties.SetProperty(pair.Key, stringValue);
			}

			properties.Store(outputStream, String.Empty);
		}
	}
}