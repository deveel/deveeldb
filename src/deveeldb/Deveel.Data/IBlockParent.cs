using System;

namespace Deveel.Data {
	interface IBlockParent {
		IBlockContext CreateBlockContext();

		IBlock CreateBlock();
	}
}
