using System;

namespace Deveel.Data {
	public interface IBlockContext : IContext {
		IBlockContext CreateBlockContext();
	}
}
