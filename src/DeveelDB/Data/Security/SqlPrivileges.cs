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

namespace Deveel.Data.Security {
	public static class SqlPrivileges {
		public static IPrivilegeResolver Resolver => new PrivilegeResolver();

		// System Privileges
		public static readonly Privilege Admin = new Privilege(2048);
		public static readonly Privilege Connect = new Privilege(4096);

		// Object Privileges
		public static readonly Privilege Create = new Privilege(1);
		public static readonly Privilege Alter = new Privilege(2);
		public static readonly Privilege Drop = new Privilege(4);
		public static readonly Privilege List = new Privilege(8);
		public static readonly Privilege Select = new Privilege(16);
		public static readonly Privilege Update = new Privilege(32);
		public static readonly Privilege Delete = new Privilege(64);
		public static readonly Privilege Insert = new Privilege(128);
		public static readonly Privilege References = new Privilege(256);
		public static readonly Privilege Usage = new Privilege(512);
		public static readonly Privilege Execute = new Privilege(1024);

		public static readonly Privilege SchemaAll;
		public static readonly Privilege SchemaRead;
		public static readonly Privilege TableAll;
		public static readonly Privilege TableRead;

		static SqlPrivileges() {
			TableAll = Select + Update + Delete + Insert + References + Usage;
			TableRead = Select + Usage;

			SchemaAll = Create + Drop + Alter + List;
			SchemaRead = List;
		}

		public static bool IsSystem(Privilege privilege) {
			return privilege.Equals(Admin) ||
			       privilege.Equals(Connect);
		}

		#region PrivilegeResolver		

		class PrivilegeResolver : IPrivilegeResolver {
			public Privilege ResolvePrivilege(string name) {
				var parts = name.Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries);
				var result = Privilege.None;

				foreach (var part in parts) {
					var privName = part.Trim();
					var priv = Privilege.None;

					switch (privName.ToUpperInvariant()) {
						case "ADMIN": priv = Admin; break;
						case "CONNECT": priv = Connect; break;

						case "SELECT": priv = Select; break;
						case "INSERT": priv = Insert; break;
						case "UPDATE": priv = Update; break;
						case "DELETE": priv = Delete; break;
						case "DROP": priv = Drop; break;
						case "REFERENCES": priv = References; break;
						case "USAGE": priv = Usage; break;
						case "CREATE": priv = Create; break;
						case "ALTER": priv = Alter; break;
						case "LIST": priv = List; break;
						case "EXECUTE": priv = Execute; break;
					}

					result += priv;
				}

				return result;
			}

			public string[] ToString(Privilege privilege) {
				var result = new List<string>();

				if (privilege.Permits(Admin))
					result.Add("ADMIN");
				if (privilege.Permits(Connect))
					result.Add("CONNECT");

				if (privilege.Permits(Select))
					result.Add("SELECT");
				if (privilege.Permits(Insert))
					result.Add("INSERT");
				if (privilege.Permits(Update))
					result.Add("UPDATE");
				if (privilege.Permits(Delete))
					result.Add("DELETE");
				if (privilege.Permits(Drop))
					result.Add("DROP");
				if (privilege.Permits(References))
					result.Add("REFERENCES");
				if (privilege.Permits(Alter))
					result.Add("ALTER");
				if (privilege.Permits(List))
					result.Add("LIST");
				if (privilege.Permits(Execute))
					result.Add("EXECUTE");
				if (privilege.Permits(Usage))
					result.Add("USAGE");
				if (privilege.Permits(Create))
					result.Add("CREATE");

				return result.ToArray();
			}
		}

		#endregion
	}
}