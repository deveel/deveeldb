using System;

namespace Deveel.Data.Build {
	public interface ISystemBuilder {
		ISystemBuilder Use(ServiceUseOptions options);

		ISystem Build();
	}
}
