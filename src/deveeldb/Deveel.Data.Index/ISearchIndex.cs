using System;
using System.Collections.Generic;

namespace Deveel.Data.Index {
	public interface ISearchIndex : IDisposable {
		void Insert(int rowIndex, string columnName, DataObject value);

		void Remove(int rowIndex, string columnName, DataObject value);

		IEnumerable<int> SelectRange(IndexRange[] ranges);
	}
}