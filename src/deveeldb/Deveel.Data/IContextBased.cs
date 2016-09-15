using System;

namespace Deveel.Data {
	public interface IContextBased {
		IContext Context { get; }
	}
}
