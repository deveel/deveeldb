using System;
using System.Collections;

namespace Deveel.Collections {
	public interface ISortedSet : ISet {
		IComparer Comparer { get; }

		object First { get; }

		object Last { get; }


		ISortedSet GetHeadList(object endElement);

		ISortedSet GetSubList(object startElement, object endElement);

		ISortedSet TailSet(object startElement);
	}
}