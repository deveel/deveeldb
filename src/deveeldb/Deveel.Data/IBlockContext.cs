using System;

using Deveel.Data.Services;

namespace Deveel.Data {
	public interface IBlockContext : IContext {
		IBlockContext CreateBlockContext();
	}
}
