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

namespace Deveel.Data.Security {
	[Serializable]
	public struct Privilege : IEquatable<Privilege> {
		public static readonly Privilege Alter = new Privilege("ALTER", 0x0100);
		public static readonly Privilege Compact = new Privilege("COMPACT", 0x040);
		public static readonly Privilege Create = new Privilege("CREATE", 0x080);
		public static readonly Privilege Delete = new Privilege("DELETE", 0x02);
		public static readonly Privilege Drop = new Privilege("DROP", 0x0200);
		public static readonly Privilege Insert = new Privilege("INSERT", 0x08);
		public static readonly Privilege List = new Privilege("LIST", 0x0400);
		public static readonly Privilege References = new Privilege("REFERENCES", 0x010);
		public static readonly Privilege Select = new Privilege("SELECT", 0x01);
		public static readonly Privilege Update = new Privilege("UPDATE", 0x04);
		public static readonly Privilege Usage = new Privilege("USAGE", 0x020);

		private Privilege(string name, int code) 
			: this() {
			Name = name;
			Code = code;
		}

		public string Name { get; private set; }

		public int Code { get; private set; }

		public bool Equals(Privilege other) {
			return Code == other.Code;
		}

		public override bool Equals(object obj) {
			if (!(obj is Privilege))
				return false;

			var other = (Privilege) obj;
			return Equals(other);
		}

		public override int GetHashCode() {
			return Code.GetHashCode();
		}

		public static Privilege GetByName(string name) {
			if (String.Equals(name, "ALTER", StringComparison.OrdinalIgnoreCase))
				return Alter;
			if (String.Equals(name, "COMPACT", StringComparison.OrdinalIgnoreCase))
				return Compact;
			if (String.Equals(name, "CREATE", StringComparison.OrdinalIgnoreCase))
				return Create;
			if (String.Equals(name, "DELETE", StringComparison.OrdinalIgnoreCase))
				return Delete;
			if (String.Equals(name, "DROP", StringComparison.OrdinalIgnoreCase))
				return Drop;
			if (String.Equals(name, "INSERT", StringComparison.OrdinalIgnoreCase))
				return Insert;
			if (String.Equals(name, "LIST", StringComparison.OrdinalIgnoreCase))
				return List;
			if (String.Equals(name, "REFERENCES", StringComparison.OrdinalIgnoreCase))
				return References;
			if (String.Equals(name, "SELECT", StringComparison.OrdinalIgnoreCase))
				return Select;
			if (String.Equals(name, "UPDATE", StringComparison.OrdinalIgnoreCase))
				return Update;
			if (String.Equals(name, "USAGE", StringComparison.OrdinalIgnoreCase))
				return Usage;

			throw new NotSupportedException(String.Format("No privilege named {0} was defined.", name));
		}

		public override string ToString() {
			return Name;
		}
	}
}