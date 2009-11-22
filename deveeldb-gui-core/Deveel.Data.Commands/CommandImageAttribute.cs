using System;

namespace Deveel.Data.Commands {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
	public sealed class CommandImageAttribute : Attribute, ICommandAttribute {
		public CommandImageAttribute(string fileName, ImageType type, ImageSource source) {
			if (fileName == null || fileName.Length == 0)
				throw new ArgumentNullException("fileName");

			this.fileName = fileName;
			this.source = source;
			this.type = type;
		}

		public CommandImageAttribute(string fileName, ImageType type)
			: this(fileName, type, ImageSource.Embedded) {
		}

		public CommandImageAttribute(string fileName, ImageSource source)
			: this(fileName, ImageType.Small, source) {
		}

		public CommandImageAttribute(string fileName)
			: this(fileName, ImageType.Small, ImageSource.Embedded) {
		}

		private ImageType type;
		private ImageSource source;
		private readonly string fileName;

		public string FileName {
			get { return fileName; }
		}

		public ImageSource Source {
			get { return source; }
			set { source = value; }
		}

		public ImageType Type {
			get { return type; }
			set { type = value; }
		}
	}
}