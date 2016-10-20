using System;

using Deveel.Data.Services;

namespace Deveel.Data.Configuration {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseConfiguration<T>(this ISystemBuilder builder, T configuration) where T : class, IConfiguration {
			return builder.Use<IConfiguration>(options => options.ToInstance(configuration));
		}

		public static ISystemBuilder UseDefaultConfiguration(this ISystemBuilder builder) {
			return builder.UseConfiguration(new Configuration());
		}

		public static ISystemBuilder UseConfiguration(this ISystemBuilder builder, Action<IConfiguration> configure) {
			var config = new Configuration();
			configure(config);
			return builder.UseConfiguration(config);
		}

		public static ISystemBuilder UseFileConfiguration(this ISystemBuilder builder, string filePath, IConfigFormatter formatter) {
			var config = new Data.Configuration.Configuration();

			using (var source = new FileConfigSource(filePath)) {
				formatter.LoadInto(config, source.InputStream);
			}

			return builder.UseConfiguration(config);
		}

		public static ISystemBuilder UseFileConfiguration(this ISystemBuilder builder, string filePath) {
			return builder.UseFileConfiguration(filePath, new PropertiesConfigFormatter());
		}
	}
}
