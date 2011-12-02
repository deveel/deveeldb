// 
//  Copyright 2009  Deveel
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

namespace Deveel.Math {
	internal class SR {
		public static string GetString(string s) {
			s = s.Replace(".", "");
			return Messages.ResourceManager.GetString(s);
		}

		public static string GetString(string s, params object[] args) {
			string format = GetString(s);
			if (format == null || format.Length == 0)
				return format;

			format = String.Format(format, args);
			return format;
		}
	}
}