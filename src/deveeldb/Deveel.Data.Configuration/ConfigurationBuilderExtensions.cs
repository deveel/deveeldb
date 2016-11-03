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

namespace Deveel.Data.Configuration {
	public static class ConfigurationBuilderExtensions {
		public static IConfigurationBuilder Load(this IConfigurationBuilder builder, IConfigurationSource source,
			IConfigurationFormatter formatter) {
			var config = new Configuration();
			formatter.LoadInto(config, source.InputStream);

			foreach (var pair in config) {
				builder = builder.WithSetting(pair.Key, pair.Value);
			}

			return builder;
		}

		public static IConfigurationBuilder Load(this IConfigurationBuilder builder, IConfigurationSource source) {
			return builder.Load(source, new PropertiesConfigurationFormatter());
		}

		public static IConfigurationBuilder LoadFile(this IConfigurationBuilder builder, string fileName,
			IConfigurationFormatter formatter) {
			using (var source = new FileConfigurationSource(fileName)) {
				return builder.Load(source, formatter);
			}
		}

		public static IConfigurationBuilder LoadFile(this IConfigurationBuilder builder, string fileName) {
			return builder.LoadFile(fileName, new PropertiesConfigurationFormatter());
		}
	}
}
