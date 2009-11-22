using System;

using Deveel.Data.Commands;

namespace Deveel.Data.Search {
	[Command("ReplaceText", "Replace Text")]
	[CommandImage("Deveel.Data.Images.text_replace.png")]
	public sealed class ReplaceTextCommand : Command {
		#region Overrides of Command

		public override void Execute() {
			ITextSearchProvider searchProvider = HostWindow.ActiveChild as ITextSearchProvider;

			if (searchProvider != null) {
				ReplaceTextSearch search = null;
				int key = searchProvider.GetHashCode();

				// is there a request in the table for this window?
				if (SearchTable.Searches.ContainsKey(key))
					search = SearchTable.Searches[key] as ReplaceTextSearch;

				if (search != null) {
					// wrap around to start if at last pos
					if (search.Position != 0)
						search.Position = searchProvider.CursorOffset;

					if (searchProvider.Replace(search.Replace, search.Position - search.Text.Length, search.Text.Length))
						Services.CommandHandler.GetCommand(typeof(FindNextCommand)).Execute();
				}
			}
		}

		#endregion
	}
}