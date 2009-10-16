using System;

namespace Deveel.Data.Control {
	/// <summary>
	/// An object the describes a single configuration property and the 
	/// default value for it.
	/// </summary>
	public sealed class ConfigProperty : ICloneable {

		private readonly string key;
		private readonly string value;
		private readonly string type;
		private readonly string comment;

		internal ConfigProperty(string key, string default_value, string type, string comment) {
			this.key = key;
			this.value = default_value;
			this.type = type;
			this.comment = comment;
		}

		internal ConfigProperty(string key, string default_value, string type)
			: this(key, default_value, type, null) {
		}

		public string Key {
			get { return key; }
		}

		public string Value {
			get { return value; }
		}

		public string Type {
			get { return type; }
		}

		public string Comment {
			get { return comment; }
		}

		public object Clone() {
			return MemberwiseClone();
		}
	}
}