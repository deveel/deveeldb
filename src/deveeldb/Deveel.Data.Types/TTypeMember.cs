using System;

namespace Deveel.Data.Types {
	public sealed class TTypeMember {
		public TTypeMember(string name, TType type, bool nullable) {
			Type = type;
			Name = name;
			IsNullable = nullable;
		}

		public string Name { get; private set; }

		public TType Type { get; private set; }

		public int Offset { get; set; }

		public bool IsNullable { get; set; }
	}
}