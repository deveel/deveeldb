using System;
using System.Collections.Generic;

namespace Deveel.Data.Transactions {
	public interface ITransactionCollection : IEnumerable<ITransaction> {
		int Count { get; }

		ITransaction FindById(long commitId);
	}
}