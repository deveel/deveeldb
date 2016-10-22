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
