using System;
using System.Windows.Forms;

using Deveel.Data.Commands;

namespace Deveel.Data.Search {
	[Command("FindNext", "Find Next")]
	[CommandShortcut(Keys.F3)]
	[CommandImage("Deveel.Data.Images.find.png")]
	public sealed class FindNextCommand : Command {
		#region Overrides of Command

		public override void Execute() {
			ITextSearchProvider searchProvider = HostWindow.ActiveChild as ITextSearchProvider;

			if (searchProvider != null) {
				TextSearch search = null;
				int key = searchProvider.GetHashCode();

				// is there a request in the table for this window?
				if (SearchTable.Searches.ContainsKey(key)) {
					search = SearchTable.Searches[key];
				} else {
					if (SearchTable.Searches.Count > 0) {
						// if there is an entry in the list of searches create an instance
						search = new TextSearch(searchProvider, null);
						search.Position = searchProvider.CursorOffset;
					} else {
						// none in table, default to curently selected text if its the editor
						IEditor editor = Editor;
						if (editor != null && editor.SelectedText.Length > 0) {
							search = new TextSearch(searchProvider, editor.SelectedText);
							search.Position = searchProvider.CursorOffset;
						}
					}
				}

				if (search != null) {
					// wrap around to start if at last pos
					if (search.Position != 0) {
						search.Position = searchProvider.CursorOffset;
					}

					search = searchProvider.TextSearchService.SearchNext(search);
					SearchTable.Searches[key] = search;
				}
			}
		}

		#endregion
	}
}