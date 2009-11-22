using System;

namespace Deveel.Data.Commands {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class CommandAttribute : Attribute, ICommandAttribute {
		public CommandAttribute(string name, string text) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (!Command.IsValidName(name))
				throw new ArgumentException();

			this.name = name;
			this.text = text;
		}

		public CommandAttribute(string name)
			: this(name, null) {
		}

		private readonly string name;
		private string text;

		public string Name {
			get { return name; }
		}

		public string Text {
			get { return text; }
			set { text = value; }
		}
	}
}