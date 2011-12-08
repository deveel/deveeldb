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

namespace Deveel.Data.Collections {
	static class ListUtil {
		public static IList<int> ToList(IIntegerList i_list) {
			if (i_list is AbstractBlockIntegerList) {
				AbstractBlockIntegerList bilist = (AbstractBlockIntegerList)i_list;
				int bill_size = bilist.Count;
				int[] bill = new int[bill_size];
				bilist.CopyToArray(bill, 0, bill_size);
				return new List<int>(bill);
			}

			List<int> list = new List<int>(i_list.Count);
			IIntegerIterator i = i_list.GetIterator();
			// NOTE: We are guarenteed the size of the 'list' array matches the size
			//   of input list.
			while (i.MoveNext()) {
				list.Add(i.Next);
			}

			return list;
		}
	}
}