// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Indexes {
	public static class IndexExtensions {
		public static IndexKey NullKey(this IIndex index)
			=> new IndexKey(index.IndexInfo.ColumnNames.Select(x => SqlObject.Null).ToArray());

		public static IEnumerable<long> SelectRange(this IIndex index, IndexRange range)
			=> index.SelectRange(new[] { range });

		public static IEnumerable<long> SelectAll(this IIndex index)
			=> index.SelectRange(IndexRange.FullRange);

		// NOTE: This will find NULL at start which is probably wrong.  The
		//   first value should be the first non null value.
		public static IEnumerable<long> SelectFirst(this IIndex index)
			=> index.SelectRange(new IndexRange(
				RangeFieldOffset.FirstValue, IndexRange.FirstInSet,
				RangeFieldOffset.LastValue, IndexRange.FirstInSet));

		public static IEnumerable<long> SelectLast(this IIndex index)
			=> index.SelectRange(new IndexRange(
				RangeFieldOffset.FirstValue, IndexRange.LastInSet,
				RangeFieldOffset.LastValue, IndexRange.LastInSet));

		public static IEnumerable<long> SelectNotNull(this IIndex index)
			=> index.SelectRange(new IndexRange(
				RangeFieldOffset.AfterLastValue, index.NullKey(),
				RangeFieldOffset.LastValue, IndexRange.LastInSet));

		public static IEnumerable<long> SelectEqual(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(new IndexRange(
				RangeFieldOffset.FirstValue, key,
				RangeFieldOffset.LastValue, key));
		}

		public static IEnumerable<long> SelectEqual(this IIndex index, SqlObject[] key)
			=> index.SelectEqual(new IndexKey(key));

		public static IEnumerable<long> SelectEqual(this IIndex index, SqlObject key)
			=> index.SelectEqual(new [] {key});

		public static IEnumerable<long> SelectNotEqual(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(new[] {
				new IndexRange(
					RangeFieldOffset.AfterLastValue, key.NullKey,
					RangeFieldOffset.BeforeFirstValue, key),
				new IndexRange(
					RangeFieldOffset.AfterLastValue, key,
					RangeFieldOffset.LastValue, IndexRange.LastInSet)
			});
		}

		public static IEnumerable<long> SelectNotEqual(this IIndex index, SqlObject[] key)
			=> index.SelectNotEqual(new IndexKey(key));

		public static IEnumerable<long> SelectNotEqual(this IIndex index, SqlObject key)
			=> index.SelectNotEqual(new[] {key});

		public static IEnumerable<long> SelectGreater(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(
				new IndexRange(
					RangeFieldOffset.AfterLastValue, key,
					RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public static IEnumerable<long> SelectGreater(this IIndex index, SqlObject[] key)
			=> index.SelectGreater(new IndexKey(key));

		public static IEnumerable<long> SelectGreater(this IIndex index, SqlObject key)
			=> index.SelectGreater(new[] {key});

		public static IEnumerable<long> SelectLess(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(new IndexRange(
				RangeFieldOffset.AfterLastValue, key.NullKey,
				RangeFieldOffset.BeforeFirstValue, key));
		}

		public static IEnumerable<long> SelectLess(this IIndex index, SqlObject[] key)
			=> index.SelectLess(new IndexKey(key));

		public static IEnumerable<long> SelectLess(this IIndex index, SqlObject key)
			=> index.SelectLess(new[] {key});

		public static IEnumerable<long> SelectGreaterOrEqual(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(new IndexRange(
				RangeFieldOffset.FirstValue, key,
				RangeFieldOffset.LastValue, IndexRange.LastInSet));
		}

		public static IEnumerable<long> SelectGreaterOrEqual(this IIndex index, SqlObject[] key)
			=> index.SelectGreaterOrEqual(new IndexKey(key));

		public static IEnumerable<long> SelectGreaterOrEqual(this IIndex index, SqlObject key)
			=> index.SelectGreaterOrEqual(new[] {key});

		public static IEnumerable<long> SelectLessOrEqual(this IIndex index, IndexKey key) {
			if (key.IsNull)
				return new long[0];

			return index.SelectRange(new IndexRange(
				RangeFieldOffset.AfterLastValue, key.NullKey,
				RangeFieldOffset.LastValue, key));
		}

		public static IEnumerable<long> SelectLessOrEqual(this IIndex index, SqlObject[] key)
			=> index.SelectLessOrEqual(new IndexKey(key));

		public static IEnumerable<long> SelectLessOrEqual(this IIndex index, SqlObject key)
			=> index.SelectLessOrEqual(new[] {key});

		public static IEnumerable<long> SelectBetween(this IIndex index, IndexKey key1, IndexKey key2) {
			if (key1.IsNull ||
			    key2.IsNull)
				return new long[0];

			return index.SelectRange(new IndexRange(
				RangeFieldOffset.FirstValue, key1,
				RangeFieldOffset.BeforeFirstValue, key2));
		}
	}
}