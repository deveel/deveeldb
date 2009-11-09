using System;

namespace Deveel.Data {
	public interface IFileEditorResolver {
		IEditor ResolveEditor(string filename);

		string ResolveEditorNameByExtension(string extension);

		void Register(FileEditorInfo editorInfo);

		FileEditorInfo[] GetFileTypes();
	}
}