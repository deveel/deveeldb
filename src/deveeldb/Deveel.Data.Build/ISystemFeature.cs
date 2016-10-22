using System;

namespace Deveel.Data.Build {
	public interface ISystemFeature {
		string Name { get; }

		string Version { get; }

		void OnBuild(ISystemBuilder builder);

		void OnSystemEvent(SystemEvent @event);
	}
}
