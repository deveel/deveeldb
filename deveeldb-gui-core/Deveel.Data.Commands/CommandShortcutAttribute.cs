using System;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class CommandShortcutAttribute : Attribute, ICommandAttribute {
		public CommandShortcutAttribute(Keys keys, string text) {
			this.keys = keys;
			this.text = text;
		}

		public CommandShortcutAttribute(Keys keys)
			: this(keys, null) {
		}

		private readonly Keys keys;
		private string text;

		public Keys Keys {
			get { return keys; }
		}

		public string Text {
			get { return text; }
			set { text = value; }
		}
	}
}