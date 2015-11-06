// 
//  Copyright 2010-2015 Deveel
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
using System.Text;

namespace Deveel.Data.Mapping {
	public class RuledNamingConvention : INamingConvention {
		public static RuledNamingConvention SqlNaming = new RuledNamingConvention(DefaultNamingRules.LowerCase | DefaultNamingRules.UnderscoreSeparator);

		public RuledNamingConvention(DefaultNamingRules rules) {
			ValidateCaseRule(rules);
			Rules = rules;
		}

		private void ValidateCaseRule(DefaultNamingRules rules) {
			if ((rules & DefaultNamingRules.CamelCase) != 0 &&
				((rules & DefaultNamingRules.LowerCase) != 0 ||
				(rules & DefaultNamingRules.UpperCase) != 0))
				throw new ArgumentException("Invalid casing rule.", "rules");

			// TODO: do the same for other casing rules
		}

		public DefaultNamingRules Rules { get; private set; }

		public string FormatName(string inputName) {
			var input = inputName.ToCharArray();
			var sb = new StringBuilder();

			bool upperSeen = false;
			for (int i = 0; i < input.Length; i++) {
				var c = input[i];
				if (Char.IsUpper(c)) {
					if ((Rules & DefaultNamingRules.LowerCase) != 0) {
						c = Char.ToLower(c);
					} else if ((Rules & DefaultNamingRules.CamelCase) != 0) {
						if (upperSeen) {
							c = Char.ToLower(c);
						}
					}

					upperSeen = true;
				} else if (Char.IsLower(c)) {
					if ((Rules & DefaultNamingRules.UpperCase) != 0) {
						c = Char.ToUpper(c);
					}

					upperSeen = false;
				}

				sb.Append(c);

				if (upperSeen && i > 0 && ((Rules & DefaultNamingRules.UnderscoreSeparator) != 0))
					sb.Append('_');
			}

			return sb.ToString();
		}
	}
}
