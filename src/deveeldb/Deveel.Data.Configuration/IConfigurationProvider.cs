using System;

namespace Deveel.Data.Configuration {
	public interface IConfigurationProvider {
		IConfiguration Configuration { get; }
	}
}
