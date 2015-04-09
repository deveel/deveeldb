using System;

namespace Deveel.Data.Transactions {
	public interface ILockable {
		object RefId { get; }


		void Released(Lock @lock);

		void Acquired(Lock @lock);
	}
}
