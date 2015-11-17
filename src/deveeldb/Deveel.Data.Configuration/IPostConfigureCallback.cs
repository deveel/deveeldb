using System;

namespace Deveel.Data.Configuration {
	public interface IPostConfigureCallback {
		void OnConfigured(IConfigurationProvider provider);
	}
}
