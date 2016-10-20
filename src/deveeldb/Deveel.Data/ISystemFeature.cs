using System;

namespace Deveel.Data {
	public interface ISystemFeature {
		string Name { get; }

		string Version { get; }

		void OnBuild(ISystemBuilder builder);
	}
}
