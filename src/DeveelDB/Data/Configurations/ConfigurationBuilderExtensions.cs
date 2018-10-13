// 
//  Copyright 2010-2018 Deveel
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
using System.Collections;
using System.IO;

namespace Deveel.Data.Configurations {
	public static class ConfigurationBuilderExtensions {
		public static IConfigurationBuilder Add(this IConfigurationBuilder builder, IConfigurationSource source, IConfigurationFormatter formatter) {
			using (var stream = source.InputStream) {
				formatter.LoadInto(builder, stream);
			}

			return builder;
		}

		public static IConfigurationBuilder AddProperties(this IConfigurationBuilder builder, IConfigurationSource source)
			=> builder.Add(source, new PropertiesFormatter());

		public static IConfigurationBuilder AddPropertiesString(this IConfigurationBuilder builder, string source)
			=> builder.AddProperties(new StringConfigurationSource(source));

		public static IConfigurationBuilder AddPropertiesFile(this IConfigurationBuilder builder, string fileName)
			=> builder.AddProperties(new FileConfigurationSource(fileName));

		public static IConfigurationBuilder AddPropertiesStream(this IConfigurationBuilder builder, Stream stream)
			=> builder.AddProperties(new StreamConfigurationSource(stream));

		public static IConfigurationBuilder AddFile(this IConfigurationBuilder builder,
			string fileName,
			IConfigurationFormatter formatter)
			=> builder.Add(new FileConfigurationSource(fileName), formatter);

		public static IConfigurationBuilder AddEnvironmentVariables(this IConfigurationBuilder builder, string prefix) {
			foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables()) {
				var key = entry.Key.ToString();
				if (key.StartsWith(prefix)) {
					key = key.Substring(prefix.Length, key.Length - prefix.Length);
					builder = builder.WithSetting(key, entry.Value);
				}
			}

			return builder;
		}

		public static IConfigurationBuilder Add(this IConfigurationBuilder builder, IConfiguration configuration) {
			foreach (var config in configuration) {
				builder = builder.WithSetting(config.Key, config.Value);
			}

			foreach (var section in configuration.Sections) {
				builder = builder.WithSection(section.Key,
					sectionBuilder => {
						foreach (var subConfig in section.Value) {
							sectionBuilder = sectionBuilder.WithSetting(subConfig.Key, subConfig.Value);
						}
					});
			}

			return builder;
		}
	}
}