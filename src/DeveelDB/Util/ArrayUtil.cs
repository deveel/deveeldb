// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Util {
	static class ArrayUtil {
		public static object[] Merge(object[] left, object[] right) {
			object[] array;
			if (right == null) {
				array = new object[left.Length];
				Array.Copy(left, array, left.Length);
			} else {
				array = new object[left.Length + right.Length];
				Array.Copy(left, 0, array, 0, left.Length);
				Array.Copy(right, 0, array, left.Length, right.Length);
			}

			return array;
		}

		public static object[] Introduce(object obj, object[] array) {
			return Merge(new object[] {obj}, array);
		}

		//public static object[] Introduce(object obj1, object obj2, object[] array) {
		//	return Merge(new[] {obj1, obj2}, array);
		//}

		//public static object[] Introduce(object obj1, object obj2, object obj3, object[] array) {
		//	return Merge(new[] {obj1, obj2, obj3}, array);
		//}
	}
}