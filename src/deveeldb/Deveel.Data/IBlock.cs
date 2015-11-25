using System;

namespace Deveel.Data {
	public interface IBlock : IRequest {
		new IBlockContext Context { get; }

		IBlock Parent { get; }

		IBlock Next { get; }

		void Execute(BlockExecuteContext context);

		IBlock CreateBlock();
	}
}
