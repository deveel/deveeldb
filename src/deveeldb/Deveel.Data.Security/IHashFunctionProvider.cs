using System;

namespace Deveel.Data.Security {
	public interface IHashFunctionProvider {
		IHashFunction ResolveFunction(string functionName);
	}
}
