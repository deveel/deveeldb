// 
//  Copyright 2010  Deveel
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

using System;
using System.Text;

namespace Deveel.Data.Query {
	/// <summary>
	///  Various helper methods for constructing a plan tree, and the plan node
	/// implementations themselves.
	/// </summary>
	class QueryPlanUtil {
		/// <summary>
		/// Replaces all elements of the array with clone versions of
		/// themselves.
		/// </summary>
		/// <param name="array"></param>
		public static void CloneArray(VariableName[] array) {
			if (array != null) {
				for (int i = 0; i < array.Length; ++i) {
					array[i] = (VariableName)array[i].Clone();
				}
			}
		}

		/// <summary>
		/// Replaces all elements of the array with clone versions of
		/// themselves.
		/// </summary>
		/// <param name="array"></param>
		public static void CloneArray(Expression[] array) {
			if (array != null) {
				for (int i = 0; i < array.Length; ++i) {
					array[i] = (Expression)array[i].Clone();
				}
			}
		}

		public static void Indent(int level, StringBuilder buf) {
			for (int i = 0; i < level; ++i) {
				buf.Append(' ');
			}
		}
	}
}