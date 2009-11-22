using System;
using System.Text;

namespace Deveel.Data {
	public sealed class FileEditorInfo {
		private readonly string key;
		private readonly string name;
		private readonly string[] extensions;

		public FileEditorInfo(string key, string name, params string[] extensions) {
			this.key = key;
			this.extensions = extensions;
			this.name = name;
		}

		public FileEditorInfo(string key, string name)
			: this(key, name, new string[0]) {
		}

		public string[] Extensions {
			get { return extensions; }
		}

		public string Name {
			get { return name; }
		}

		public string Key {
			get { return key; }
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder(Name);
			if (extensions.Length > 0) {
				sb.Append("( ");
				for (int i = 0; i < extensions.Length; i++) {
					sb.Append("*.");
					sb.Append(extensions[i]);
					if (i < extensions.Length - 1)
						sb.Append("; ");
				}
				sb.Append(" )");
			}

			return sb.ToString();
		}
	}
}