using System;

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
	}
}