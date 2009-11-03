using System;
using System.Collections;

namespace Deveel.Collections {
	public interface ISortedDictionary : IDictionary {
		IComparer Comparer { get; }

		object FirstKey { get; }

		object LastKey { get; }

		bool ContainsKey(object key);

		bool ContainsValue(object value);

		ISortedDictionary GetHeadDictionary(object endKey);

		ISortedDictionary GetSubDictionary(object startKey, object endKey);

		ISortedDictionary TailDictionary(object startKey);
	}
}