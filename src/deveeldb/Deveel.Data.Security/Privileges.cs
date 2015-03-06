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
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Security {
	[Serializable]
	public sealed class Privileges {
		private readonly IDictionary<int, Privilege> privs;

		public Privileges() {
			privs = new Dictionary<int, Privilege>();
		}

		public bool IsEmpty {
			get { return privs.Count == 0; }
		}

		public Privileges Add(Privilege privilege) {
			if (!privs.ContainsKey(privilege.Code))
				privs[privilege.Code] = privilege;

			return this;
		}

		public Privileges Add(string privilegeName) {
			return Add(Privilege.GetByName(privilegeName));
		}

		public Privileges Remove(Privilege privilege) {
			privs.Remove(privilege.Code);
			return this;
		}

		public bool Permits(Privilege privilege) {
			return privs.ContainsKey(privilege.Code);
		}

		public Privileges Merge(Privileges privileges) {
			var result = new Privileges();
			foreach (var privilege in privs) {
				result.privs[privilege.Key] = privilege.Value;
			}

			foreach (var privilege in privileges.privs) {
				if (!result.privs.ContainsKey(privilege.Key))
					result.privs[privilege.Key] = privilege.Value;
			}

			return result;
		}

		public override string ToString() {
			var sb = new StringBuilder();

			int i = 0;
			foreach (var privilege in privs) {
				sb.Append(privilege.Value.Name);
				if (++i < privs.Count - 1)
					sb.Append("|");
			}

			return sb.ToString();
		}

		public static Privileges Parse(string s) {
			if (String.IsNullOrEmpty(s))
				return new Privileges();

			var result = new Privileges();
			var sp = s.Split(new[] {'|'}, StringSplitOptions.RemoveEmptyEntries);

			for (int i = 0; i < sp.Length; i++) {
				var name = sp[i].Trim();
				Privilege privilege;

				try {
					privilege = Privilege.GetByName(name);
				} catch (Exception ex) {
					throw new FormatException(String.Format("Could not get a valie privilege for '{0}'", name));
				}

				result.Add(privilege);
			}

			return result;
		}
	}
}