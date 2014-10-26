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

namespace Deveel.Data.Configuration {
	public static class DbConfigExtensions {
				public static void Load(this IDbConfig config, IConfigSource source) {
			config.Load(source, new PropertiesConfigFormatter());
		}

		public static void Load(this IDbConfig config, IConfigFormatter formatter) {
			if (config.Source == null)
				throw new InvalidOperationException("Source was not configured");

			config.Load(config.Source, formatter);
		}

		public static void Load(this IDbConfig config, IConfigSource source, IConfigFormatter formatter) {
			try {
				if (source != null) {
					using (var sourceStream = source.InputStream) {
						if (!sourceStream.CanRead)
							throw new ArgumentException("The input stream cannot be read.");

						sourceStream.Seek(0, SeekOrigin.Begin);
						formatter.LoadInto(config, sourceStream);
					}
				}
			} catch (Exception ex) {
				throw new DatabaseConfigurationException(String.Format("Cannot load data from source"), ex);
			}
		}

		public static void Load(this IDbConfig config, string fileName, IConfigFormatter formatter) {
			config.Load(new FileConfigSource(fileName), formatter);
		}

		public static void Load(this IDbConfig config, string fileName) {
			config.Load(fileName, new PropertiesConfigFormatter());
		}

		public static void Load(this IDbConfig config, Stream inputStream, IConfigFormatter formatter) {
			config.Load(new StreamConfigSource(inputStream), formatter);
		}

		public static void Load(this IDbConfig config, Stream inputStream) {
			config.Load(inputStream, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, IConfigSource source, IConfigFormatter formatter) {
			Save(config, source, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IDbConfig config, IConfigSource source, ConfigurationLevel level, IConfigFormatter formatter) {
			try {
				using (var outputStream = source.OutputStream) {
					if (!outputStream.CanWrite)
						throw new InvalidOperationException("The destination source cannot be written.");

					outputStream.Seek(0, SeekOrigin.Begin);
					formatter.SaveFrom(config, level, outputStream);
					outputStream.Flush();
				}
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Cannot save the configuration.", ex);
			}
		}

		public static void Save(this IDbConfig config, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, IConfigFormatter formatter) {
			if (config.Source == null)
				throw new DatabaseConfigurationException("The source was not configured in the configuration.");

			config.Save(config.Source, level, formatter);
		}

		public static void Save(this IDbConfig config) {
			Save(config, ConfigurationLevel.Current);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level) {
			Save(config, level, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName) {
			Save(config, ConfigurationLevel.Current, fileName);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, string fileName) {
			Save(config, level, fileName, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, string fileName, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, fileName, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, string fileName, IConfigFormatter formatter) {
			config.Save(new FileConfigSource(fileName), level, formatter);
		}

		public static void Save(this IDbConfig config, Stream outputStream) {
			Save(config, ConfigurationLevel.Current, outputStream);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, Stream outputStream) {
			Save(config, level, outputStream, new PropertiesConfigFormatter());
		}

		public static void Save(this IDbConfig config, Stream outputStream, IConfigFormatter formatter) {
			Save(config, ConfigurationLevel.Current, outputStream, formatter);
		}

		public static void Save(this IDbConfig config, ConfigurationLevel level, Stream outputStream, IConfigFormatter formatter) {
			config.Save(new StreamConfigSource(outputStream), level, formatter);
		}
	}
}