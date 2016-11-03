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

using Deveel.Data.Build;
using Deveel.Data.Services;

namespace Deveel.Data.Configuration {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseConfiguration<T>(this ISystemBuilder builder, T configuration) where T : class, IConfiguration {
			return builder.Use<IConfiguration>(options => options.With(configuration).Replace());
		}

		public static ISystemBuilder UseDefaultConfiguration(this ISystemBuilder builder) {
			return builder.UseConfiguration(new Configuration());
		}

		public static ISystemBuilder UseConfiguration(this ISystemBuilder builder, Action<IConfigurationBuilder> configure) {
			var configBuilder = new ConfigurationBuilder();
			configure(configBuilder);
			return builder.UseConfiguration(configBuilder.Build());
		}

		public static ISystemBuilder UseFileConfiguration(this ISystemBuilder builder, string filePath, IConfigurationFormatter formatter) {
			var config = new Data.Configuration.Configuration();

			using (var source = new FileConfigurationSource(filePath)) {
				formatter.LoadInto(config, source.InputStream);
			}

			return builder.UseConfiguration(config);
		}

		public static ISystemBuilder UseFileConfiguration(this ISystemBuilder builder, string filePath) {
			return builder.UseFileConfiguration(filePath, new PropertiesConfigurationFormatter());
		}
	}
}
