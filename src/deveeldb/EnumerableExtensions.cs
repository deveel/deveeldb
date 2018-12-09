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
using System.Text;

namespace Deveel
{
	public static class EnumerableExtensions {
		public static BigArray<T> ToBigArray<T>(this IEnumerable<T> source) {
			if (source is BigArray<T>)
				return (BigArray<T>) source;

			var size = source.LongCount();
			var array = new BigArray<T>(size);
			long index = 0;
			foreach (var item in source) {
				array[index++] = item;
			}

			return array;
		}

		public static BigList<T> ToBigList<T>(this IEnumerable<T> source) {
			if (source is BigList<T>)
				return (BigList<T>) source;

			return new BigList<T>(source);
		}

		public static T ElementAt<T>(this IEnumerable<T> source, long offset) {
			if (source is BigArray<T>) {
				return ((BigArray<T>) source)[offset];
			}

			if (source is BigList<T>)
				return ((BigList<T>) source)[offset];

			if (source is IList<T> && offset < Int32.MaxValue)
				return ((IList<T>) source)[(int) offset];

			long index = 0;
			foreach (var item in source) {
				if (index == offset)
					return item;

				index++;
			}

			throw new ArgumentOutOfRangeException(nameof(offset), "The offset is past the enumeration size");
		}
	}
}
