using System;

namespace Deveel.Data.Security {
	public interface IKeyedHashFunction : IHashFunction {
		byte[] Key { get; set; }
	}
}