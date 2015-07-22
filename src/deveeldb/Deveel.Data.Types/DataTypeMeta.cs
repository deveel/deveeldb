using System;

namespace Deveel.Data.Types {
	public sealed class DataTypeMeta {
		internal DataTypeMeta(string name, string value) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Value = value;
		}

		public string Name { get; private set; }

		public string Value { get; private set; }

		public int ToInt32() {
			return Convert.ToInt32(Value);
		}

		public long ToInt64() {
			return Convert.ToInt64(Value);
		}
	}
}
