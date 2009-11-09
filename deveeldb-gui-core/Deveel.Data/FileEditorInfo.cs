using System;

namespace Deveel.Data {
	public sealed class FileEditorInfo {
		private readonly string key;
		private readonly string name;
		private readonly string[] extensions;

		public FileEditorInfo(string key, string name, string[] extensions) {
			this.key = key;
			this.extensions = extensions;
			this.name = name;
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