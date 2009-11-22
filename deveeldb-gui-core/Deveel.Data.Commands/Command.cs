using System;
using System.Drawing;
using System.IO;
using System.Reflection;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public abstract class Command : ICommand {
		protected Command(string name, Keys shortcut, string shortcutText) {
			if ((name != null && name.Length > 0) && !IsValidName(name))
				throw new ArgumentException();

			this.name = name;
			this.shortcut = shortcut;
			this.shortcutText = shortcutText;

			RetrieveCommandAttributes();
		}

		protected Command(string name, string shortcutText)
			: this(name, Keys.None, shortcutText) {
		}

		protected Command(string name)
			: this(name, null) {
		}

		protected Command()
			: this(null, Keys.None, null) {
		}

		private Keys shortcut;
		private string shortcutText;
		private string name;
		private string text;
		private IHostWindow hostWindow;
		private Image smallImage;
		private IApplicationServices services;
		private ISettings settings;

		public string Name {
			get { return name; }
		}

		public string Text {
			get { return text == null ? name : text; }
		}

		public Keys Shortcut {
			get { return shortcut; }
		}

		public virtual bool Enabled {
			get { return true; }
		}

		public IApplicationServices Services {
			get { return services; }
		}

		protected ISettings Settings {
			get { return settings; }
		}

		protected IHostWindow HostWindow {
			get {
				if (hostWindow == null)
					hostWindow = services.HostWindow;
				return hostWindow;
			}
		}

		public string ShortcutText {
			get { return shortcutText; }
		}

		public Image SmallImage {
			get { return smallImage; }
		}

		protected IEditor Editor {
			get { return HostWindow.ActiveChild as IEditor; }
		}

		protected IQueryEditor QueryEditor {
			get { return HostWindow.ActiveChild as IQueryEditor; }
		}

		private Image RetrieveImage(string fileName, ImageSource source) {
			//TODO: support for ImageSource.Resource...
			if (source == ImageSource.Resource)
				return null;

			Assembly assembly = Assembly.GetAssembly(GetType());
			Stream stream = assembly.GetManifestResourceStream(fileName);
			if (stream == null || stream == Stream.Null)
				return null;

			try {
				Image sourceImage = Image.FromStream(stream);
				Bitmap image = new Bitmap(sourceImage.Width, sourceImage.Height);
				Graphics g = Graphics.FromImage(image);
				g.DrawImage(sourceImage, 0, 0);
				return image;
			} finally {
				stream.Close();
			}
		}

		private void RetrieveCommandAttributes() {
			object[] attrs = GetType().GetCustomAttributes(typeof(ICommandAttribute), false);
			for (int i = 0; i < attrs.Length; i++) {
				object attr = attrs[i];

				if (attr is CommandAttribute) {
					CommandAttribute commandAttr = (CommandAttribute) attr;
					name = commandAttr.Name;
					text = commandAttr.Text;
				} else if (attr is CommandImageAttribute) {
					CommandImageAttribute commandImageAttr = (CommandImageAttribute) attr;
					if (commandImageAttr.Type == ImageType.Small) {
						smallImage = RetrieveImage(commandImageAttr.FileName, commandImageAttr.Source);
					} else {
						//TODO:
					}
				} else if (attr is CommandShortcutAttribute) {
					CommandShortcutAttribute commandScutAttr = (CommandShortcutAttribute) attr;
					shortcut = commandScutAttr.Keys;
					shortcutText = commandScutAttr.Text;
				}
			}
		}

		internal static bool IsValidName(string name) {
			if (Char.IsNumber(name[0]))
				return false;

			for (int i = 1; i < name.Length; i++) {
				if (!Char.IsLetterOrDigit(name[i]))
					return false;
			}

			return true;
		}

		protected void SetSmallImage(Image image) {
			smallImage = image;
		}

		internal void SetSettings(ISettings appSettings) {
			settings = appSettings;
		}

		internal void SetServices(IApplicationServices appServices) {
			services = appServices;
		}

		public abstract void Execute();
	}
}