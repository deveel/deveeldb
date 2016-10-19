using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	public interface ISystemBuilder {
		ServiceContainer ServiceContainer { get; }

		ISystem Build();
	}
}
