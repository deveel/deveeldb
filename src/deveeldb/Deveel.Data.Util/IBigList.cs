using System;
using System.Collections.Generic;

namespace Deveel.Data.Util {
	public interface IBigList<T> : IList<T> {
		T this[long index] { get; set; }

		long BigCount { get; }

		void Insert(long index, T item);

		void RemoveAt(long index);
	}
}
