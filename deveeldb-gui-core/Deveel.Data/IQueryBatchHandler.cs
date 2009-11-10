using System;

namespace Deveel.Data {
	public interface IQueryBatchHandler {
		SqlQueryBatch Batch { get; }
	}
}