using System;
using System.Drawing;
using System.IO;
using System.Reflection;
using System.Windows.Forms;

namespace Deveel.Data.Commands {
	public abstract class Command : ICommand {
		protected Command(string name, Keys shortcut, string shortcutText) {
			this.name = name;
			this.shortcut = shortcut;
			this.shortcutText = shortcutText;

			smallImage = RetrieveSmallImage();
		}

		protected Command(string name, string shortcutText)
			: this(name, Keys.None, shortcutText) {
		}

		protected Command(string name)
			: this(name, null) {
		}

		private readonly Keys shortcut;
		private readonly string shortcutText;
		private readonly string name;
		private IHostWindow hostWindow;
		private Image smallImage;
		private IApplicationServices services;
		private ISettings settings;

		public string Name {
			get { return name; }
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

		private Image RetrieveSmallImage() {
			CommandSmallImageAttribute attribute =
				Attribute.GetCustomAttribute(GetType(), typeof (CommandSmallImageAttribute)) 
				as CommandSmallImageAttribute;
			if (attribute == null)
				return null;

			//TODO: support for ImageSource.Resource...

			string fileName = attribute.FileName;
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