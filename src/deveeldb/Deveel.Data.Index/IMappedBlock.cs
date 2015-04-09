using System;

using Deveel.Data.Store;

namespace Deveel.Data.Index {
	interface IMappedBlock : IIndexBlock<int> {
		int FirstEntry { get; }

		int LastEntry { get; }

		long BlockPointer { get; }

		byte CompactType { get; }


		long CopyTo(IStore destStore);

		long Flush();
	}
}