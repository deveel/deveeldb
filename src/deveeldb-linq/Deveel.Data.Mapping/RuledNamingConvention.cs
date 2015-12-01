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
