using System;

namespace Deveel.Data.Commands {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class CommandSmallImageAttribute : Attribute {
		public CommandSmallImageAttribute(string fileName, ImageSource source) {
			if (fileName == null || fileName.Length == 0)
				throw new ArgumentNullException("fileName");

			this.fileName = fileName;
			this.source = source;
		}

		public CommandSmallImageAttribute(string fileName)
			: this(fileName, ImageSource.Embedded) {
		}

		private ImageSource source;
		private readonly string fileName;

		public string FileName {
			get { return fileName; }
		}

		public ImageSource Source {
			get { return source; }
			set { source = value; }
		}
	}
}