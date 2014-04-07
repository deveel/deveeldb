using System;

namespace Deveel.Data.Security {
	public interface IHashFunction : IDisposable {
		int HashSize { get; }


		byte[] Compute(byte[] data);

		void Clear();
	}
}