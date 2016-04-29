using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Linq {
	public sealed class Grouping<TKey, TElement> : IGrouping<TKey, TElement> {
		private readonly TKey key;
		private IEnumerable<TElement> group;

		public Grouping(TKey key, IEnumerable<TElement> @group) {
			this.key = key;
			this.group = group;
		}

		public IEnumerator<TElement> GetEnumerator() {
			if (!(@group is List<TElement>))
				@group = @group.ToList();

			return @group.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public TKey Key {
			get { return key; }
		}
	}
}
