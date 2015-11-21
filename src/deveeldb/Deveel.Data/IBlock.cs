using System;

namespace Deveel.Data {
	public interface IBlock : IDisposable {
		IBlockContext Context { get; }

		IBlock Parent { get; }

		IBlock CreateBlock();
	}
}
