using System;

namespace Deveel.Data.Configuration {
	public interface IConfigurationBuilder {
		IConfigurationBuilder WithSetting(string key, object value);

		IConfiguration Build();
	}
}
