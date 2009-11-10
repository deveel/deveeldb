using System;
using System.Collections;
using System.IO;

namespace Deveel.Data {
	public sealed class FileEditorResolver : IFileEditorResolver {
		public FileEditorResolver(IApplicationServices services) {
			this.services = services;
			extentions = new Hashtable();
			fileEditorInfo = new ArrayList();
		}


		private readonly IApplicationServices services;
		private readonly Hashtable extentions;
		private readonly ArrayList fileEditorInfo;


		#region Implementation of IFileEditorResolver

		public IEditor ResolveEditor(string filename) {
			string ext = Path.GetExtension(filename);
			string editorName = ResolveEditorNameByExtension(ext);
			return (IEditor) services.Resolve(editorName, typeof(IEditor));
		}

		public string ResolveEditorNameByExtension(string extension) {
			string editorName = ((FileEditorInfo)extentions["*"]).Key;

			if (extension != null) {
				if (extension.StartsWith("."))
					extension = extension.Substring(1);

				if (extentions.ContainsKey(extension))
					editorName = ((FileEditorInfo)extentions[extension]).Key;
			}

			return editorName;
		}

		public void Register(FileEditorInfo editorInfo) {
			fileEditorInfo.Add(editorInfo);
			if (editorInfo.Extensions == null || editorInfo.Extensions.Length == 0) {
				extentions.Add("*", editorInfo);
			} else {
				foreach (string extention in editorInfo.Extensions)
					extentions.Add(extention, editorInfo);
			}
		}

		public FileEditorInfo[] GetFileTypes() {
			return (FileEditorInfo[]) (new ArrayList(extentions.Values)).ToArray(typeof (FileEditorInfo));
		}

		#endregion
	}
}