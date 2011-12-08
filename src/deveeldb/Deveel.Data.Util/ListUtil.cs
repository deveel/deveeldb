// 
//  Copyright 2011  Deveel
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

using System.Collections.Generic;

namespace Deveel.Data.Util {
	static class ListUtil {
		public static IList<int> ToList(IIndex index) {
			/*
			if (index is BlockIndexBase) {
				BlockIndexBase bilist = (BlockIndexBase)index;
				int bill_size = bilist.Count;
				int[] bill = new int[bill_size];
				bilist.CopyTo(bill, 0, bill_size);
				return new List<int>(bill);
			}
			*/

			List<int> list = new List<int>(index.Count);
			IIndexEnumerator i = index.GetEnumerator();
			// NOTE: We are guarenteed the size of the 'list' array matches the size
			//   of input list.
			while (i.MoveNext()) {
				list.Add(i.Current);
			}

			return list;
		}
	}
}