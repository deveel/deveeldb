using System;

namespace Deveel.Data.Transactions {
	public interface ITableEvent : ITransactionEvent {
		int TableId { get; }
	}
}
