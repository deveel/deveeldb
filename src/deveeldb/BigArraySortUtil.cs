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

namespace Deveel {
	static class BigArraySortUtil<T> {
		private static readonly IComparer<T> comparer = new ItemComparer();

		private class ItemComparer : IComparer<T> {
			public int Compare(T x, T y) {
				if (x is IComparable<T>) {
					var a = (IComparable<T>)x;
					return a.CompareTo(y);
				} else if (x is IComparable) {
					var a = (IComparable)x;
					return a.CompareTo(y);
				} else {
					throw new NotSupportedException();
				}
			}
		}

		public static void QuickSort(BigArray<T> elements, long left, long right) {
			long i = left, j = right;
			var pivot = elements[(left + right) / 2];

			while (i <= j) {
				while (comparer.Compare(elements[i], pivot) < 0) {
					i++;
				}

				while (comparer.Compare(elements[j], pivot) > 0) {
					j--;
				}

				if (i <= j) {
					// Swap
					var tmp = elements[i];
					elements[i] = elements[j];
					elements[j] = tmp;

					i++;
					j--;
				}
			}

			// Recursive calls
			if (left < j) {
				QuickSort(elements, left, j);
			}

			if (i < right) {
				QuickSort(elements, i, right);
			}
		}
	}
}